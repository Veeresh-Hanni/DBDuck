"""
Query Builder with Model Relationships

Demonstrates the Query Builder API with UModel relationships:
- ForeignKey (Many-to-One)
- OneToMany
- ManyToMany

Uses Django-style model definitions from DBDuck.models.
"""

from DBDuck import UDOM
from DBDuck.models import (
    BooleanField,
    CharField,
    Column,
    ForeignKey,
    IntegerField,
    ManyToMany,
    OneToMany,
    UModel,
    CASCADE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Model Definitions with Relationships
# ─────────────────────────────────────────────────────────────────────────────

class Author(UModel):
    """Author model - can have many books."""
    __entity__ = "authors"
    
    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False)
    email = Column(CharField, unique=True)
    active = Column(BooleanField, default=True)
    
    # One author has many books
    books = OneToMany("Book", back_populates="author")


class Publisher(UModel):
    """Publisher model."""
    __entity__ = "publishers"
    
    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False)
    country = Column(CharField)


class Book(UModel):
    """Book model with relationships."""
    __entity__ = "books"
    
    id = Column(IntegerField, primary_key=True)
    title = Column(CharField, nullable=False)
    isbn = Column(CharField, unique=True)
    year_published = Column(IntegerField)
    
    # Many books belong to one author
    author_id = ForeignKey(Author, on_delete=CASCADE)
    
    # Many books belong to one publisher
    publisher_id = ForeignKey(Publisher, on_delete=CASCADE)


class Tag(UModel):
    """Tag model for book categories."""
    __entity__ = "tags"
    
    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False, unique=True)


class BookTag(UModel):
    """Junction table for Book-Tag many-to-many relationship."""
    __entity__ = "book_tags"
    
    book_id = ForeignKey(Book, on_delete=CASCADE)
    tag_id = ForeignKey(Tag, on_delete=CASCADE)


# ─────────────────────────────────────────────────────────────────────────────
# Setup Database
# ─────────────────────────────────────────────────────────────────────────────

def setup_database():
    """Create database and tables."""
    db = UDOM(url="sqlite:///:memory:")
    
    # Create tables
    db.adapter.run_native("""
        CREATE TABLE authors (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            active INTEGER DEFAULT 1
        )
    """)
    
    db.adapter.run_native("""
        CREATE TABLE publishers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            country TEXT
        )
    """)
    
    db.adapter.run_native("""
        CREATE TABLE books (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            isbn TEXT UNIQUE,
            year_published INTEGER,
            author_id INTEGER REFERENCES authors(id),
            publisher_id INTEGER REFERENCES publishers(id)
        )
    """)
    
    db.adapter.run_native("""
        CREATE TABLE tags (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
    """)
    
    db.adapter.run_native("""
        CREATE TABLE book_tags (
            book_id INTEGER REFERENCES books(id),
            tag_id INTEGER REFERENCES tags(id),
            PRIMARY KEY (book_id, tag_id)
        )
    """)
    
    # Bind models
    Author.bind(db)
    Publisher.bind(db)
    Book.bind(db)
    Tag.bind(db)
    BookTag.bind(db)
    
    return db


# ─────────────────────────────────────────────────────────────────────────────
# Insert Sample Data
# ─────────────────────────────────────────────────────────────────────────────

def insert_sample_data(db):
    """Insert sample data using Query Builder."""
    print("Inserting sample data...")
    
    # Authors
    db.table("authors").create({"id": 1, "name": "Jane Austen", "email": "jane@austen.com", "active": 1})
    db.table("authors").create({"id": 2, "name": "Charles Dickens", "email": "charles@dickens.com", "active": 1})
    db.table("authors").create({"id": 3, "name": "Mark Twain", "email": "mark@twain.com", "active": 0})
    
    # Publishers
    db.table("publishers").create({"id": 1, "name": "Penguin Books", "country": "UK"})
    db.table("publishers").create({"id": 2, "name": "HarperCollins", "country": "US"})
    
    # Books
    db.table("books").create({
        "id": 1, 
        "title": "Pride and Prejudice", 
        "isbn": "978-0141439518", 
        "year_published": 1813, 
        "author_id": 1, 
        "publisher_id": 1
    })
    db.table("books").create({
        "id": 2, 
        "title": "Sense and Sensibility", 
        "isbn": "978-0141439662", 
        "year_published": 1811, 
        "author_id": 1, 
        "publisher_id": 1
    })
    db.table("books").create({
        "id": 3, 
        "title": "Oliver Twist", 
        "isbn": "978-0141439747", 
        "year_published": 1838, 
        "author_id": 2, 
        "publisher_id": 1
    })
    db.table("books").create({
        "id": 4, 
        "title": "Great Expectations", 
        "isbn": "978-0141439563", 
        "year_published": 1861, 
        "author_id": 2, 
        "publisher_id": 2
    })
    db.table("books").create({
        "id": 5, 
        "title": "Tom Sawyer", 
        "isbn": "978-0143107330", 
        "year_published": 1876, 
        "author_id": 3, 
        "publisher_id": 2
    })
    
    # Tags
    db.table("tags").create({"id": 1, "name": "Classic"})
    db.table("tags").create({"id": 2, "name": "Romance"})
    db.table("tags").create({"id": 3, "name": "Drama"})
    db.table("tags").create({"id": 4, "name": "Adventure"})
    
    # Book-Tag associations
    db.table("book_tags").create({"book_id": 1, "tag_id": 1})  # Pride - Classic
    db.table("book_tags").create({"book_id": 1, "tag_id": 2})  # Pride - Romance
    db.table("book_tags").create({"book_id": 2, "tag_id": 1})  # Sense - Classic
    db.table("book_tags").create({"book_id": 2, "tag_id": 2})  # Sense - Romance
    db.table("book_tags").create({"book_id": 3, "tag_id": 1})  # Oliver - Classic
    db.table("book_tags").create({"book_id": 3, "tag_id": 3})  # Oliver - Drama
    db.table("book_tags").create({"book_id": 4, "tag_id": 1})  # Great - Classic
    db.table("book_tags").create({"book_id": 4, "tag_id": 3})  # Great - Drama
    db.table("book_tags").create({"book_id": 5, "tag_id": 1})  # Tom - Classic
    db.table("book_tags").create({"book_id": 5, "tag_id": 4})  # Tom - Adventure
    
    print("✓ Sample data inserted\n")


# ─────────────────────────────────────────────────────────────────────────────
# Query Examples with Relationships
# ─────────────────────────────────────────────────────────────────────────────

def demonstrate_queries(db):
    """Demonstrate Query Builder with related models."""
    
    print("=" * 60)
    print("Query Builder with Model Relationships")
    print("=" * 60)
    
    # ─────────────────────────────────────────────────────────────────────────
    # 1. Basic Queries on Related Models
    # ─────────────────────────────────────────────────────────────────────────
    print("\n1. Basic Queries")
    print("-" * 40)
    
    # All authors
    authors = Author.query().find()
    print(f"All authors: {[a.name for a in authors]}")
    
    # Active authors only
    active_authors = Author.query().where(active=1).find()
    print(f"Active authors: {[a.name for a in active_authors]}")
    
    # All books ordered by year
    books = Book.query().order("year_published").find()
    print(f"Books by year: {[b.title for b in books]}")
    
    # ─────────────────────────────────────────────────────────────────────────
    # 2. Querying with Foreign Keys
    # ─────────────────────────────────────────────────────────────────────────
    print("\n2. Foreign Key Queries")
    print("-" * 40)
    
    # Books by a specific author (author_id = 1, Jane Austen)
    jane_books = Book.query().where(author_id=1).find()
    print(f"Jane Austen's books: {[b.title for b in jane_books]}")
    
    # Books by publisher (Penguin Books, publisher_id = 1)
    penguin_books = Book.query().where(publisher_id=1).find()
    print(f"Penguin Books titles: {[b.title for b in penguin_books]}")
    
    # Books by author AND publisher
    filtered = Book.query().where(author_id=2, publisher_id=1).find()
    print(f"Dickens books from Penguin: {[b.title for b in filtered]}")
    
    # ─────────────────────────────────────────────────────────────────────────
    # 3. Join-like Queries (manual with multiple queries)
    # ─────────────────────────────────────────────────────────────────────────
    print("\n3. Multi-table Queries")
    print("-" * 40)
    
    # Get author name for a book
    book = Book.query().where(id=1).first()
    author = Author.query().where(id=book.author_id).first()
    print(f"'{book.title}' by {author.name}")
    
    # Get all books with their author names
    all_books = Book.query().find()
    for book in all_books:
        author = Author.query().where(id=book.author_id).first()
        publisher = Publisher.query().where(id=book.publisher_id).first()
        print(f"  - '{book.title}' by {author.name} ({publisher.name})")
    
    # ─────────────────────────────────────────────────────────────────────────
    # 4. Many-to-Many Queries (via junction table)
    # ─────────────────────────────────────────────────────────────────────────
    print("\n4. Many-to-Many Queries")
    print("-" * 40)
    
    # Get tags for a specific book
    book = Book.query().where(id=1).first()
    book_tags = BookTag.query().where(book_id=book.id).find()
    tags = [Tag.query().where(id=bt.tag_id).first() for bt in book_tags]
    tag_names = [t.name for t in tags]
    print(f"Tags for '{book.title}': {tag_names}")
    
    # Find all books with "Romance" tag
    romance_tag = Tag.query().where(name="Romance").first()
    romance_book_tags = BookTag.query().where(tag_id=romance_tag.id).find()
    romance_books = [Book.query().where(id=bt.book_id).first() for bt in romance_book_tags]
    print(f"Romance books: {[b.title for b in romance_books]}")
    
    # Find all books with "Classic" tag (should be all)
    classic_tag = Tag.query().where(name="Classic").first()
    classic_count = BookTag.query().where(tag_id=classic_tag.id).count()
    print(f"Books with 'Classic' tag: {classic_count}")
    
    # ─────────────────────────────────────────────────────────────────────────
    # 5. Aggregation and Counting
    # ─────────────────────────────────────────────────────────────────────────
    print("\n5. Aggregation Queries")
    print("-" * 40)
    
    # Count books per author
    for author in Author.query().find():
        count = Book.query().where(author_id=author.id).count()
        print(f"  {author.name}: {count} books")
    
    # Count books per publisher
    for publisher in Publisher.query().find():
        count = Book.query().where(publisher_id=publisher.id).count()
        print(f"  {publisher.name}: {count} books")
    
    # ─────────────────────────────────────────────────────────────────────────
    # 6. Update and Delete with Relationships
    # ─────────────────────────────────────────────────────────────────────────
    print("\n6. Update/Delete Operations")
    print("-" * 40)
    
    # Update author status
    Author.query().where(id=3).update({"active": 1})
    mark = Author.query().where(id=3).first()
    print(f"Updated Mark Twain active status: {bool(mark.active)}")
    
    # Check if book exists before operations
    exists = Book.query().where(title="Pride and Prejudice").exists()
    print(f"Pride and Prejudice exists: {exists}")
    
    # Add a new book tag
    db.table("book_tags").create({"book_id": 3, "tag_id": 4})  # Oliver - Adventure
    oliver_tags = BookTag.query().where(book_id=3).count()
    print(f"Oliver Twist now has {oliver_tags} tags")
    
    # Delete a book tag
    BookTag.query().where(book_id=3, tag_id=4).delete()
    oliver_tags = BookTag.query().where(book_id=3).count()
    print(f"Oliver Twist now has {oliver_tags} tags after delete")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """Run relationship examples."""
    print("=" * 60)
    print("DBDuck Query Builder - Model Relationships Example")
    print("=" * 60)
    print()
    
    # Setup
    db = setup_database()
    insert_sample_data(db)
    
    # Run demos
    demonstrate_queries(db)
    
    print("\n" + "=" * 60)
    print("✓ All relationship queries completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
