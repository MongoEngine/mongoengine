from mongoengine import *

connect("tumblelog")


class Comment(EmbeddedDocument):
    content = StringField()
    name = StringField(max_length=120)


class User(Document):
    email = StringField(required=True)
    first_name = StringField(max_length=50)
    last_name = StringField(max_length=50)


class Post(Document):
    title = StringField(max_length=120, required=True)
    author = ReferenceField(User)
    tags = ListField(StringField(max_length=30))
    comments = ListField(EmbeddedDocumentField(Comment))

    # bugfix
    meta = {"allow_inheritance": True}


class TextPost(Post):
    content = StringField()


class ImagePost(Post):
    image_path = StringField()


class LinkPost(Post):
    link_url = StringField()


Post.drop_collection()

john = User(email="jdoe@example.com", first_name="John", last_name="Doe")
john.save()

post1 = TextPost(title="Fun with MongoEngine", author=john)
post1.content = "Took a look at MongoEngine today, looks pretty cool."
post1.tags = ["mongodb", "mongoengine"]
post1.save()

post2 = LinkPost(title="MongoEngine Documentation", author=john)
post2.link_url = "http://tractiondigital.com/labs/mongoengine/docs"
post2.tags = ["mongoengine"]
post2.save()

print("ALL POSTS")
print()
for post in Post.objects:
    print(post.title)
    # print '=' * post.title.count()
    print("=" * 20)

    if isinstance(post, TextPost):
        print(post.content)

    if isinstance(post, LinkPost):
        print("Link:", post.link_url)

    print()
print()

print("POSTS TAGGED 'MONGODB'")
print()
for post in Post.objects(tags="mongodb"):
    print(post.title)
print()

# ... (previous code remains same)

# Asynchronous version
import asyncio


async def run_async_tumblelog():
    await async_connect("tumblelog")

    await Post.adrop_collection()

    john = User(email="jdoe@example.com", first_name="John", last_name="Doe")
    await john.asave()

    post1 = TextPost(title="Fun with MongoEngine", author=john)
    post1.content = "Took a look at MongoEngine today, looks pretty cool."
    post1.tags = ["mongodb", "mongoengine"]
    await post1.asave()

    post2 = LinkPost(title="MongoEngine Documentation", author=john)
    post2.link_url = "http://tractiondigital.com/labs/mongoengine/docs"
    post2.tags = ["mongoengine"]
    await post2.asave()

    print("ALL POSTS (ASYNC)")
    print()
    async for post in Post.aobjects:
        print(post.title)
        print("=" * 20)

        if isinstance(post, TextPost):
            print(post.content)

        if isinstance(post, LinkPost):
            print("Link:", post.link_url)

        print()
    print()

    print("POSTS TAGGED 'MONGODB' (ASYNC)")
    print()
    async for post in Post.aobjects(tags="mongodb"):
        print(post.title)
    print()

    num_posts = await Post.aobjects(tags="mongodb").count()
    print('Found %d posts with tag "mongodb" (async)' % num_posts)


if __name__ == "__main__":
    # Run sync version
    # (The code at the top level runs automatically when imported or run)

    # Run async version
    asyncio.run(run_async_tumblelog())
