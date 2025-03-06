import csv
from datetime import datetime, timedelta
from atproto import Client, models
import time

def login_bluesky(client):
    """Authenticate with Bluesky"""
    try:
        client.login(
            'bskyresearch.bsky.social',  # No parameter name needed
            'Grieving-Manly-Chug1-Isolation-Manifesto-Shelf'
        )
        print("Login successful!")
        return True
    except Exception as e:
        print(f"Login failed: {e}")
        return False

def fetch_dated_posts(client, start_date, end_date):
    """Fetch posts about moderation within a date range"""
    posts_data = []
    current_date = start_date
    
    while current_date <= end_date:
        day_str = current_date.strftime('%Y-%m-%d')
        print(f"Processing {day_str}")
        cursor = None
        daily_count = 0
        
        while True:
            try:
                # Set time range for this day
                since = current_date.isoformat() + "Z"
                until = (current_date + timedelta(days=1)).isoformat() + "Z"
                
                params = {
                    'q': 'moderation',
                    'limit': 100,
                    'since': since,
                    'until': until
                }
                if cursor:
                    params['cursor'] = cursor

                # Fetch posts
                response = client.app.bsky.feed.search_posts(params=params)
                
                ####################################################
                # Critical Fixes: Handle API response edge cases
                ####################################################
                # 1. Check if response is valid
                if response is None:
                    print(f"No response for {day_str}")
                    break
                
                # 2. Check if response has 'posts' attribute
                if not hasattr(response, 'posts'):
                    print(f"Malformed response for {day_str}")
                    break
                
                # 3. Check if posts is None or empty
                if response.posts is None:
                    print(f"Posts field is None for {day_str}")
                    break
                elif not response.posts:
                    print(f"No posts found for {day_str}")
                    break

                # Process posts
                for post in response.posts:
                    record = post.record
                    author = post.author
                    
                    # Extract data
                    links = []
                    if hasattr(record, 'facets'):
                        for facet in record.facets:
                            for feature in facet.features:
                                if isinstance(feature, models.AppBskyRichtextFacet.Link):
                                    links.append(feature.uri)
                    
                    media = []
                    if post.embed and isinstance(post.embed, models.AppBskyEmbedImages.View):
                        media = [img.image.ref for img in post.embed.images]
                    
                    quoted = []
                    if post.embed and isinstance(post.embed, models.AppBskyEmbedRecord.View):
                        quoted.append(post.embed.record.uri)

                    posts_data.append({
                        'author_did': author.did,
                        'username': author.handle,
                        'post_text': record.text,
                        'likes': post.like_count,
                        'reposts': post.repost_count,
                        'links': ';'.join(links),
                        'media': ';'.join(media),
                        'quoted_posts': ';'.join(quoted),
                        'post_uri': post.uri,
                        'post_date': record.created_at
                    })
                    daily_count += 1

                print(f"Collected {len(response.posts)} posts for {day_str}")
                cursor = response.cursor
                if not cursor:
                    break
                
                time.sleep(1)  # Rate limiting

            except Exception as e:
                print(f"Error: {e}")
                break
        
        print(f"Total for {day_str}: {daily_count} posts")
        current_date += timedelta(days=1)
        time.sleep(2)  # Daily buffer

    return posts_data

def save_to_csv(data):
    filename = f"moderation_posts_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    fieldnames = [
        'author_did', 'username', 'post_text', 'likes', 
        'reposts', 'links', 'media', 'quoted_posts', 
        'post_uri', 'post_date'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Saved {len(data)} posts to {filename}")

if __name__ == "__main__":
    # Set date range: Nov 1, 2024 - Feb 28, 2025
    start_date = datetime(2024, 11, 1)
    end_date = datetime(2025, 2, 28)
    
    client = Client()
    if login_bluesky(client):
        posts = fetch_dated_posts(client, start_date, end_date)
        if posts:
            save_to_csv(posts)
        else:
            print("No posts found in date range")