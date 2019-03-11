"""Models linking API responses to the database.

`dataclass` is used to define models, with logic for derived fields
going in the model's `__post_init__` method. Derived fields are fields
that exist in the database, but not the API response, and are computed
based on fields that do exist in the API response. To include these
in the model, mark the derived field using `field(init=False)` in the
model declaration, and also add the fields from which it is derived
to the model using `InitVar`. If the database table has fields which
are autogenerated, do not declare them in the model.

As an example, we could use a derived field to rename something
from the API response.

The database table:
```sql
create table posts (
    id integer primary key autoincrement,
    author varchar not null,
    content varchar not null
);
```

The API response:
```python
data = {
    "user": "Joe",
    "content": "Wow, the new GoT episode is crazy!",
}
```

The model:
```python
@dataclass
class Post:
    # Exclude `id` as it is generated by the database.
    author: str = field(init=False)
    user: InitVar[str]
    content: str

    def __post_init__(self, user: str, **_):
        self.author = user


post = Post(**data)
```
"""

from dataclasses import InitVar, dataclass, field
from typing import Optional


@dataclass
class Submission:
    id: str
    title: str
    author_fullname: str
    author: str
    subreddit: str
    permalink: str
    created_utc: int

    selftext_html: Optional[str]
    num_comments: InitVar[int]
    comments: int = field(init=False)
    gilded: int
    downs: int
    ups: int
    score: int

    search_query: str

    def __post_init__(self, num_comments: int, **_):
        self.comments = num_comments

@dataclass
class Media:
    submission_id: str
    url: str
    is_direct: bool
    txt: Optional[str]

@dataclass
class Album:
    id: str
    media_id: int
    title: Optional[str]
    description: Optional[str]
    datetime: InitVar[int]
    uploaded_utc: int = field(init=False)
    link: InitVar[str]
    url: str = field(init=False)
    views: int

    def __post_init__(self, datetime: int, link: str, **_):
        self.uploaded_utc = datetime
        self.url = link

@dataclass
class Image:
    id: str
    media_id: int
    album_id: Optional[str]
    title: Optional[str]
    description: Optional[str]
    datetime: InitVar[Optional[int]]
    uploaded_utc: Optional[int] = field(init=False)
    type: InitVar[Optional[str]]
    mimetype: Optional[str] = field(init=False)
    link: InitVar[str]
    url: str = field(init=False)
    views: Optional[int]
    img: Optional[bytes]

    def __post_init__(self, datetime: int, type: str, link: str, **_):
        self.uploaded_utc = datetime
        self.mimetype = type
        self.url = link

@dataclass
class ProductSearchResult:
    brandName: InitVar[str]
    brand: str = field(init=False)
    productId: InitVar[str]
    product_id: int = field(init=False)
    productName: InitVar[str]
    product_name: str = field(init=False)
    categoryFacet: InitVar[str]
    category: str = field(init=False)
    search_query: str

    def __post_init__(self, brandName: str, productId: str, productName: str, categoryFacet: str, **_):  # noqa: E501
        self.brand = brandName
        self.product_id = int(productId)
        self.product_name = productName
        self.category = categoryFacet

@dataclass
class Product:
    id: int
    brandName: InitVar[str]
    brand: str = field(init=False)
    productName: InitVar[str]
    name: str = field(init=False)
    defaultProductUrl: InitVar[str]
    default_url: str = field(init=False)
    description: Optional[str]

    def __post_init__(self, brandName: str, productName: str, defaultProductUrl: str, **_):  # noqa: E501
        self.brand = brandName
        self.name = productName
        self.default_url = defaultProductUrl
