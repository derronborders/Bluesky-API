import requests
import csv
from datetime import datetime, timedelta
import time
import re
from pathlib import Path

##################################################
# AUTHENTICATION SETUP
##################################################

BLUESKY_HANDLE = "bskyresearch.bsky.social"
APP_PASSWORD = "Grieving-Manly-Chug1-Isolation-Manifesto-Shelf"

def get_auth_token():
    """Authenticate with Bluesky servers and return access token"""
    url = "https://bsky.social/xrpc/com.atproto.server.createSession"
    data = {
        "identifier": BLUESKY_HANDLE,
        "password": APP_PASSWORD
    }
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        return response.json()['accessJwt']
    except Exception as e:
        print(f"Authentication failed: {str(e)}")
        return None

HEADERS = {
    "User-Agent": "ResearchBot/1.0 (Epistemic Threat Analysis)",
    "Authorization": f"Bearer {get_auth_token()}"
}

##################################################
# CORE API FUNCTIONS
##################################################

def search_bluesky_posts(query, sort="latest", since=None, until=None, lang=None, limit=100, cursor=None):
    """Search posts with authentication"""
    url = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"
    params = {
        "q": query,
        "sort": sort,
        "since": since,
        "until": until,
        "lang": lang,
        "limit": limit,
        "cursor": cursor,
    }
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("posts", []), data.get("cursor", None)
    except Exception as e:
        print(f"Search error: {str(e)}")
        return [], None

def fetch_thread_details(uri):
    """Fetch thread details for a given post URI"""
    if not uri:
        return None
    url = "https://bsky.social/xrpc/app.bsky.feed.getPostThread"
    params = {"uri": uri}
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            thread = response.json()
            if "thread" in thread and "post" in thread["thread"]:
                return thread["thread"]
        return None
    except Exception as e:
        print(f"Thread fetch error: {str(e)}")
        return None

##################################################
# THREAD PROCESSING FUNCTIONS
##################################################

def find_subtree_for_target(node, target_uri):
    """Find the subtree containing the target post in a thread"""
    if not node or "post" not in node:
        return None
    if node["post"].get("uri", "") == target_uri:
        return node
    for r in node.get("replies", []):
        subtree = find_subtree_for_target(r, target_uri)
        if subtree:
            return subtree
    return None

def climb_to_top_level(start_post):
    """Climb upwards to find the original post in the thread"""
    current_post = start_post
    while True:
        record = current_post.get("record", {})
        reply_info = record.get("reply", {})
        if not reply_info:
            break
        parent_uri = reply_info.get("parent", {}).get("uri", "")
        if not parent_uri:
            break
        parent_thread = fetch_thread_details(parent_uri)
        if not parent_thread or "post" not in parent_thread:
            break
        parent_post = parent_thread["post"]
        current_post = parent_post
    return current_post

def fetch_full_thread(uri, depth=3):
    """Recursively fetch thread with parent/child relationships"""
    def fetch_replies(node, current_depth):
        if current_depth >= depth:
            return node
        if "replies" in node:
            for i, reply in enumerate(node["replies"]):
                if "post" in reply:
                    reply_uri = reply["post"].get("uri")
                    reply_data = fetch_thread_details(reply_uri)
                    if reply_data:
                        node["replies"][i] = fetch_replies(reply_data, current_depth+1)
        return node
    
    base_thread = fetch_thread_details(uri)
    return fetch_replies(base_thread, 0) if base_thread else None

def find_path_in_thread(thread_node, target_uri):
    """Find the conversation path to a specific post"""
    if not thread_node:
        return None
    
    stack = [(thread_node, [])]
    while stack:
        node, path = stack.pop()
        if "post" not in node:
            continue
        post_obj = node["post"]
        new_path = path + [post_obj]
        if post_obj.get("uri", "") == target_uri:
            return new_path
        for r in node.get("replies", []):
            stack.append((r, new_path))
    return None

##################################################
# DATA PROCESSING FUNCTIONS
##################################################

def extract_post_metrics(post):
    """Extract engagement metrics from a post"""
    return {
        "likes": post.get("likeCount", 0),
        "replies": post.get("replyCount", 0),
        "reposts": post.get("repostCount", 0),
        "quotes": post.get("quoteCount", 0),
        "created_at": post.get("record", {}).get("createdAt", "")
    }

def process_thread_to_row(target_post):
    """Structure an entire thread into a single CSV row with metrics"""
    # Climb to the root of the thread
    root_post = climb_to_top_level(target_post)
    if not root_post:
        return None
    
    # Fetch full thread structure
    thread = fetch_full_thread(root_post["uri"])
    if not thread:
        return None
    
    # Find path from root to target post
    chain = find_path_in_thread(thread, target_post["uri"])
    if not chain:
        return None
    
    # Initialize row structure
    row = {
        "thread_root_uri": root_post["uri"],
        "thread_root_author": root_post.get("author", {}).get("handle", ""),
        "target_post_uri": target_post["uri"],
        "target_post_author": target_post.get("author", {}).get("handle", ""),
        "target_post_text": target_post.get("record", {}).get("text", "")[:200] + "...",
    }
    
    # Add target post metrics
    target_metrics = extract_post_metrics(target_post)
    row.update({f"target_{k}": v for k, v in target_metrics.items()})
    
    # Add parent posts (ancestors of target)
    for i, parent in enumerate(chain[:-1]):
        prefix = f"parent_{i+1}_"
        metrics = extract_post_metrics(parent)
        row.update({
            f"{prefix}uri": parent["uri"],
            f"{prefix}author": parent.get("author", {}).get("handle", ""),
            f"{prefix}text": parent.get("record", {}).get("text", "")[:150] + "...",
            **{f"{prefix}{k}": v for k, v in metrics.items()}
        })
    
    # Add child posts (replies to target)
    target_subtree = find_subtree_for_target(thread, target_post["uri"])
    if target_subtree and "replies" in target_subtree:
        for i, reply in enumerate(target_subtree["replies"][:3]):  # First 3 replies
            if "post" in reply:
                reply_post = reply["post"]
                prefix = f"reply_{i+1}_"
                metrics = extract_post_metrics(reply_post)
                row.update({
                    f"{prefix}uri": reply_post["uri"],
                    f"{prefix}author": reply_post.get("author", {}).get("handle", ""),
                    f"{prefix}text": reply_post.get("record", {}).get("text", "")[:150] + "...",
                    **{f"{prefix}{k}": v for k, v in metrics.items()}
                })
    
    return row

##################################################
# DATA SAVING FUNCTION
##################################################

def save_to_csv(rows, filename):
    """Save with hierarchical columns and metrics"""
    fieldnames = [
        "thread_root_uri", "thread_root_author",
        "target_post_uri", "target_post_author", "target_post_text",
        "target_likes", "target_replies", "target_reposts", "target_quotes", "target_created_at"
    ]
    
    # Add parent columns
    for i in range(1, 4):
        fieldnames += [
            f"parent_{i}_uri", f"parent_{i}_author", f"parent_{i}_text",
            f"parent_{i}_likes", f"parent_{i}_replies", f"parent_{i}_reposts",
            f"parent_{i}_quotes", f"parent_{i}_created_at"
        ]
    
    # Add reply columns
    for i in range(1, 4):
        fieldnames += [
            f"reply_{i}_uri", f"reply_{i}_author", f"reply_{i}_text",
            f"reply_{i}_likes", f"reply_{i}_replies", f"reply_{i}_reposts",
            f"reply_{i}_quotes", f"reply_{i}_created_at"
        ]
    
    # Create directory if needed
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

##################################################
# MAIN EXECUTION
##################################################

if __name__ == "__main__":
    variations = ["echo chamber"]
    social_platforms = ["bluesky"]
    
    # Updated date range to November 11, 2024
    start_date = datetime(2024, 11, 11)
    end_date = datetime(2024, 11, 12)
    delta = timedelta(days=1)
    total_days = (end_date - start_date).days + 1

    final_rows = []

    print(f"🚀 Starting scrape from {start_date.date()} to {end_date.date()}")
    print(f"🔍 Searching for: {variations[0]} mentioning {social_platforms[0]}")
    print("━" * 60)

    for query in variations:
        current_date = start_date
        processed_days = 0
        
        while current_date <= end_date:
            processed_days += 1
            date_str = current_date.strftime("%Y-%m-%d")
            
            print(f"\n📅 Processing {date_str}")
            print("━" * 40)
            
            since = current_date.isoformat() + "Z"
            until = (current_date + delta).isoformat() + "Z"
            
            cursor = None
            while True:
                posts, cursor = search_bluesky_posts(
                    f'"{query}"', "latest", since, until, "en", 100, cursor
                )
                
                if not posts:
                    print(f"✅ No posts found for {date_str}")
                    break

                for post in posts:
                    post_text = post.get("record", {}).get("text", "").lower()
                    if any(re.search(rf'\b{re.escape(p)}\b', post_text) for p in social_platforms):
                        thread_row = process_thread_to_row(post)
                        if thread_row:
                            final_rows.append(thread_row)
                            print(f"📥 Added thread: {thread_row['target_post_uri']}")
                
                if not cursor:
                    break
                
                time.sleep(5)
            
            current_date += delta
            time.sleep(5)

    save_to_csv(final_rows, "thread_conversations.csv")
    print(f"\n🎉 Saved {len(final_rows)} threads to CSV")