import requests  # type: ignore
import csv
from datetime import datetime, timedelta
import time

# Function to fetch posts
def search_bluesky_posts(query, sort="latest", since=None, until=None, lang=None, limit=100, cursor=None):
    url = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
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
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            return data.get("posts", []), data.get("cursor", None)
        else:
            print(f"Error: Received status code {response.status_code}")
            return [], None
    except requests.exceptions.RequestException as e:
        print(f"Network error occurred: {e}")
        return [], None

# Function to fetch thread details
def fetch_thread_details(uri):
    if not uri:
        return None
    url = "https://public.api.bsky.app/xrpc/app.bsky.feed.getPostThread"
    params = {"uri": uri}
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            thread = response.json()
            if "thread" in thread and "post" in thread["thread"]:
                return thread["thread"]
        else:
            print(f"Skipping URI {uri} due to status code {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Skipping URI {uri} due to network error: {e}")
        return None

# Updated function to extract embeds
def extract_embeds(record, embed_type, did=None):
    # Check if "embed" exists and is not empty
    if "embed" not in record or not record["embed"]:
        return []  # No embed data available

    embed = record["embed"]

    # Use the provided DID if available, fallback to the record DID
    did = did or record.get("did", "").strip()
    if did.startswith("did:"):
        did = did[4:]  # Remove redundant "did:" if already included

    # Handle image embeds
    if embed_type == "image" and embed.get("$type") == "app.bsky.embed.images":
        images = embed.get("images", [])
        return [
            f"https://cdn.bsky.app/img/feed_thumbnail/plain/did:{did}/{image['image']['ref']['$link']}@{image['image']['mimeType'].split('/')[-1]}"
            for image in images
            if "image" in image and "ref" in image["image"] and "mimeType" in image["image"]
        ]

    # Handle external (website) embeds
    elif embed_type == "link" and embed.get("$type") == "app.bsky.embed.external":
        external = embed.get("external", {})
        uri = external.get("uri", "")
        if uri:
            return [uri]  # Return the external link

    # Handle record embeds (referenced posts)
    elif embed_type == "record" and embed.get("$type") == "app.bsky.embed.record":
        record = embed.get("record", {})
        uri = record.get("uri", "")
        if uri.startswith("at://"):
            profile = uri.split("/")[2]
            post_id = uri.split("/")[-1]
            return [f"https://bsky.app/profile/{profile}/post/{post_id}"]
        return []

    # Handle media links within nested records
    if embed.get("$type") == "app.bsky.embed.recordWithMedia":
        nested_embeds = []
        if "record" in embed:
            nested_embeds.extend(extract_embeds(embed["record"], embed_type, did))
        if "media" in embed:
            nested_embeds.extend(extract_embeds(embed["media"], embed_type, did))
        return nested_embeds

    # If no valid data is found
    return []

# Function to extract post data
def extract_post_data(post):
    if not post:
        return None
    record = post.get("record", {})
    author = post.get("author", {})

    # Construct link to the post itself
    post_uri = record.get("uri", "")
    post_link = ""
    if post_uri.startswith("at://"):
        profile = post_uri.split("/")[2]
        post_id = post_uri.split("/")[-1]
        post_link = f"https://bsky.app/profile/{profile}/post/{post_id}"

    # Construct link to the author's profile
    handle = author.get("handle", "")
    handle_link = f"https://bsky.app/profile/{handle}" if handle else ""

    return {
        "Post Link": post_link,
        "DID": author.get("did", ""),
        "Handle": handle_link,
        "Display Name": author.get("displayName", ""),
        "CreatedAt": record.get("createdAt", ""),
        "Text": record.get("text", ""),
        "Text Post Link": post_link,
        "ReplyCount": post.get("replyCount", 0),
        "RepostCount": post.get("repostCount", 0),
        "LikeCount": post.get("likeCount", 0),
        "QuoteCount": post.get("quoteCount", 0),
        "Image Embeds": ", ".join(extract_embeds(record, "image", author.get("did", ""))),
        "Website Card Embeds": ", ".join(extract_embeds(record, "link")),
        "Referenced Posts": ", ".join(extract_embeds(record, "record")),
    }

# Function to process the post, parent, and replies
def process_thread(post):
    if not post:
        return None

    # Extract Target Post
    target_post = extract_post_data(post)

    # Extract Immediate Parent
    parent_uri = post.get("record", {}).get("reply", {}).get("parent", {}).get("uri", "")
    parent_post = None
    if parent_uri:
        parent_thread = fetch_thread_details(parent_uri)
        if parent_thread and "post" in parent_thread:
            parent_post = extract_post_data(parent_thread["post"])

    # Extract Replies (children of the target post)
    replies = []
    thread_details = fetch_thread_details(post.get("uri"))
    if thread_details and "replies" in thread_details:
        for reply in thread_details["replies"]:
            if "post" in reply:
                replies.append(extract_post_data(reply["post"]))

    return {
        "Target": target_post,
        "Parent": parent_post,
        "Replies": replies,
    }

# Function to save data to CSV
def save_to_csv(data, filename):
    # Flatten data into a grouped structure
    flat_data = []
    for thread in data:
        row = {}

        # Add Target Post
        if thread["Target"]:
            for key, value in thread["Target"].items():
                row[f"Target_{key}"] = value

        # Add Parent Post
        if thread["Parent"]:
            for key, value in thread["Parent"].items():
                row[f"Parent_{key}"] = value

        # Add Replies
        for i, reply in enumerate(thread["Replies"], start=1):
            for key, value in reply.items():
                row[f"Reply_{i}_{key}"] = value

        flat_data.append(row)

    # Dynamically collect all keys (fieldnames) from all rows
    all_keys = set()
    for row in flat_data:
        all_keys.update(row.keys())
    all_keys = sorted(all_keys)  # Sort for consistent ordering

    # Ensure all rows contain all keys (fill missing fields with None)
    for row in flat_data:
        for key in all_keys:
            if key not in row:
                row[key] = None

    # Save to CSV
    with open(filename, "w", newline="", encoding="utf-8") as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=all_keys)
        dict_writer.writeheader()
        dict_writer.writerows(flat_data)

    print(f"Data saved to {filename}")

# Main script
if __name__ == "__main__":
    variations = ["example", "example variation"]
    sort = "latest"
    lang = "en"
    limit = 100

    # Set date range
    start_date = datetime(2023, 7, 1)
    end_date = datetime(2024, 12, 22)
    delta = timedelta(days=1)

    threads = []

    # Loop through query variations
    for query in variations:
        query = f'"{query}"'
        print(f"Processing query: {query}")

        # Loop through each day
        current_date = start_date
        while current_date <= end_date:
            since = current_date.isoformat() + "Z"
            until = (current_date + delta).isoformat() + "Z"
            print(f"Fetching posts from {since} to {until}")

            cursor = None
            while True:
                # Fetch posts for the current day
                posts, cursor = search_bluesky_posts(query, sort, since, until, lang, limit, cursor)
                if not posts:
                    print("No more posts for this query and date range.")
                    break

                # Process each post
                for post in posts:
                    thread_data = process_thread(post)
                    if thread_data:
                        threads.append(thread_data)

                if not cursor:
                    break
                time.sleep(5)  # Avoid rate-limiting

            current_date += delta

    # Save to CSV
    save_to_csv(threads, "bluesky_raw_data.csv")
    print("Script complete.")
