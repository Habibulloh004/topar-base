# Topar Mobile API Documentation

API reference for the mobile/web app.

**Base URL:** `https://klasstovar.uz/api`

**Authentication:** JWT Bearer token — include `Authorization: Bearer <token>` on all protected endpoints.

---

## Error Responses

All errors return JSON with an `error` field:

```json
{ "error": "description of what went wrong" }
```

| HTTP Status | Meaning |
|-------------|---------|
| `400` | Bad request / invalid input |
| `401` | Missing or invalid token |
| `403` | Forbidden (not your resource) |
| `404` | Resource not found |
| `409` | Conflict (duplicate) |
| `500` | Internal server error |

---

## Health Check

### `GET /health`

```json
{ "status": "ok", "database": "topar", "redis": true }
```

---

## Categories

### `GET /categories`

Returns the full category tree.

```json
{
  "collection": "main_categories",
  "data": [
    {
      "id": "64a1b2c3d4e5f6a7b8c9d0e1",
      "name": "Книги",
      "parentId": "0",
      "children": [
        {
          "id": "64a1b2c3d4e5f6a7b8c9d0e2",
          "name": "Детские книги",
          "parentId": "64a1b2c3d4e5f6a7b8c9d0e1",
          "children": []
        }
      ]
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | ObjectID hex |
| `name` | string | Category name |
| `parentId` | string | Parent ObjectID hex, or `"0"` for root |
| `children` | array | Nested child categories (recursive) |

---

## Products

### `GET /mainProducts`

Returns a paginated list of products with optional filters.

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | integer | `1` | Page number |
| `limit` | integer | `20` | Items per page (max `200`) |
| `search` | string | — | Full-text search by name, ISBN, author |
| `categoryId` | string | — | Filter by single category ObjectID |
| `categoryIds` | string | — | Comma-separated category ObjectIDs |
| `withoutCategory` | boolean | `false` | Only products with no category |
| `withoutIsbn` | boolean | `false` | Only products without ISBN |
| `infoComplete` | string | — | `"yes"` or `"no"` |
| `billzSync` | string | — | `"syncable"` or `"not-syncable"` |

**Response**

```json
{
  "collection": "main_products",
  "pagination": { "page": 1, "limit": 20, "totalItems": 1500, "totalPages": 75 },
  "data": [
    {
      "id": "64a1b2c3d4e5f6a7b8c9d0e1",
      "name": "Гарри Поттер и философский камень",
      "isbn": "978-5-389-07435-4",
      "authorCover": "Дж. К. Роулинг",
      "authorNames": ["Дж. К. Роулинг"],
      "tagNames": ["фэнтези"],
      "genreNames": ["Фэнтези"],
      "isInfoComplete": true,
      "description": "...",
      "annotation": "...",
      "coverUrl": "https://klasstovar.uz/uploads/main-products/image.jpg",
      "covers": { "manual_1": "https://klasstovar.uz/uploads/main-products/image.jpg" },
      "pages": 432,
      "format": "84x108/32",
      "paperType": "офсетная",
      "bindingType": "твёрдый",
      "ageRestriction": "6+",
      "characteristics": "...",
      "productType": "book",
      "subjectName": "Художественная литература",
      "nicheName": "Детская литература",
      "brandName": "",
      "seriesName": "Гарри Поттер",
      "publicationYear": 2022,
      "productWeight": "500г",
      "publisherName": "Росмэн",
      "ikpu": "10303200001000000",
      "size": "200x130",
      "quantity": 45,
      "price": 85000,
      "categoryId": "64a1b2c3d4e5f6a7b8c9d0e2",
      "categoryPath": ["Книги", "Детские книги"],
      "createdAt": "2024-01-15T09:30:00Z",
      "updatedAt": "2024-04-01T12:00:00Z"
    }
  ]
}
```

---

## Authentication

### `POST /api/auth/register`

Create a new user account and receive a JWT.

**Request**
```json
{
  "email": "user@example.com",
  "password": "secret123",
  "firstName": "Адил",
  "lastName": "Каримов",
  "displayName": "Адил К.",
  "phone": "+998901234567"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `email` | ✅ | Must be unique |
| `password` | ✅ | Minimum 6 characters |
| `firstName` | — | — |
| `lastName` | — | — |
| `displayName` | — | Defaults to `firstName + lastName` |
| `phone` | — | — |

**Response** `201 Created`
```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "user": {
    "id": "64a1b2c3d4e5f6a7b8c9d0e1",
    "email": "user@example.com",
    "phone": "+998901234567",
    "firstName": "Адил",
    "lastName": "Каримов",
    "displayName": "Адил К.",
    "avatar": "",
    "isActive": true,
    "createdAt": "2024-01-15T09:30:00Z",
    "updatedAt": "2024-01-15T09:30:00Z"
  }
}
```

---

### `POST /api/auth/login`

Authenticate and receive a JWT.

**Request**
```json
{ "email": "user@example.com", "password": "secret123" }
```

**Response** `200 OK`
```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "user": { ...same as register... }
}
```

---

### `POST /api/auth/logout` 🔒

Invalidate the session client-side. Returns `200 OK`.

```json
{ "message": "logged out" }
```

> JWT tokens are stateless — the client should discard the stored token on logout.

---

## User Profile

All `/api/users/me` endpoints require `Authorization: Bearer <token>`.

### `GET /api/users/me` 🔒

Returns the authenticated user's profile.

```json
{
  "id": "64a1b2c3d4e5f6a7b8c9d0e1",
  "email": "user@example.com",
  "phone": "+998901234567",
  "firstName": "Адил",
  "lastName": "Каримов",
  "displayName": "Адил К.",
  "avatar": "",
  "isActive": true,
  "createdAt": "2024-01-15T09:30:00Z",
  "updatedAt": "2024-01-15T09:30:00Z"
}
```

---

### `PUT /api/users/me` 🔒

Update profile fields. Send only the fields you want to change (all optional).

**Request**
```json
{
  "firstName": "Адил",
  "lastName": "Каримов",
  "displayName": "Адил К.",
  "phone": "+998901234567"
}
```

**Response** `200 OK` — returns updated user object.

---

### `PUT /api/users/me/password` 🔒

Change password.

**Request**
```json
{
  "oldPassword": "current_password",
  "newPassword": "new_password_min_6"
}
```

**Response** `200 OK`
```json
{ "message": "password updated" }
```

---

## Addresses

### `GET /api/users/me/addresses` 🔒

Returns all saved delivery addresses for the user.

```json
{
  "data": [
    {
      "id": "64a1b2c3d4e5f6a7b8c9d0e1",
      "userId": "64a1b2c3d4e5f6a7b8c9d0e0",
      "type": "home",
      "city": "Ташкент",
      "district": "Яккасарайский",
      "addressText": "ул. Навои, д. 5, кв. 12",
      "isDefault": true,
      "createdAt": "2024-01-15T09:30:00Z",
      "updatedAt": "2024-01-15T09:30:00Z"
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | `"home"`, `"work"`, or `"other"` |
| `isDefault` | boolean | Whether this is the default address |

---

### `POST /api/users/me/addresses` 🔒

Add a new delivery address.

**Request**
```json
{
  "type": "home",
  "city": "Ташкент",
  "district": "Яккасарайский",
  "addressText": "ул. Навои, д. 5, кв. 12",
  "isDefault": true
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `city` | ✅ | — |
| `addressText` | ✅ | Full address string |
| `type` | — | Defaults to `"other"` |
| `district` | — | — |
| `isDefault` | — | Defaults to `false` |

**Response** `201 Created` — returns the created address object.

---

### `PUT /api/users/me/addresses/:id` 🔒

Update an existing address. Send only fields to change.

**Request** — same fields as POST, all optional.

**Response** `200 OK` — returns updated address object.

---

### `DELETE /api/users/me/addresses/:id` 🔒

Delete an address.

**Response** `200 OK`
```json
{ "message": "address deleted" }
```

---

## Orders

### `POST /api/orders` 🔒

Place an order from the current cart. On success, the backend creates the order, clears the cart, and adds ordered products to the user's library.

**Request**
```json
{
  "paymentMethod": "card",
  "deliveryAddressId": "64a1b2c3d4e5f6a7b8c9d0e1",
  "deliveryAmount": 50000,
  "bonusAmount": 0,
  "comments": "Позвоните перед доставкой"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `paymentMethod` | ✅ | Required. Example: `"card"`, `"cash"`, `"payme"` |
| `deliveryAddressId` | — | ObjectID of a saved address that belongs to the authenticated user |
| `deliveryAmount` | — | Delivery fee in UZS. Must be `0` or greater |
| `bonusAmount` | — | Bonus points applied. Must be `0` or greater |
| `comments` | — | Free-text order note |

**Validation notes**

- Returns `400` if `paymentMethod` is missing.
- Returns `400` if `deliveryAddressId` is invalid or not found.
- Returns `403` if `deliveryAddressId` belongs to another user.
- Returns `400` if `deliveryAmount` or `bonusAmount` is negative.
- Returns `400` if the cart does not exist or has no items.

**Response** `201 Created`
```json
{
  "id": "64a1b2c3d4e5f6a7b8c9d0f0",
  "userId": "64a1b2c3d4e5f6a7b8c9d0e0",
  "items": [
    {
      "productId": "64a1b2c3d4e5f6a7b8c9d0e1",
      "name": "Гарри Поттер",
      "price": 85000,
      "quantity": 1
    }
  ],
  "totalAmount": 85000,
  "deliveryAmount": 50000,
  "bonusAmount": 0,
  "paymentMethod": "card",
  "deliveryAddressId": "64a1b2c3d4e5f6a7b8c9d0e1",
  "comments": "Позвоните перед доставкой",
  "status": "pending",
  "createdAt": "2024-01-15T09:30:00Z",
  "updatedAt": "2024-01-15T09:30:00Z"
}
```

**Order status values:** `pending` → `processing` → `shipped` → `delivered` / `cancelled`

---

### `GET /api/orders` 🔒

List the authenticated user's orders (paginated).

**Query Parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `page` | `1` | Page number |
| `limit` | `20` | Items per page (max `100`) |

**Response** `200 OK`
```json
{
  "data": [ ...order objects... ],
  "total": 5,
  "page": 1,
  "limit": 20
}
```

---

### `GET /api/orders/:id` 🔒

Get a single order by ID (must belong to the authenticated user).

**Response** `200 OK` — returns the order object.

---

### `GET /api/users/me/orders` 🔒

Alias for `GET /api/orders` — same response format and query parameters. Useful for profile page integration.

---

## Cart

All `/api/cart` endpoints require `Authorization: Bearer <token>`.

A cart is created automatically when first accessed.

### `GET /api/cart` 🔒

Returns the current cart (creates it if it doesn't exist).

```json
{
  "id": "64a1b2c3d4e5f6a7b8c9d0f1",
  "userId": "64a1b2c3d4e5f6a7b8c9d0e0",
  "items": [
    {
      "productId": "64a1b2c3d4e5f6a7b8c9d0e1",
      "quantity": 2,
      "priceAtAdd": 85000
    }
  ],
  "updatedAt": "2024-01-15T09:30:00Z"
}
```

---

### `POST /api/cart/items` 🔒

Add a product to the cart. If the product is already in the cart, its quantity is **incremented** by the given amount.

**Request**
```json
{ "productId": "64a1b2c3d4e5f6a7b8c9d0e1", "quantity": 1 }
```

| Field | Required | Description |
|-------|----------|-------------|
| `productId` | ✅ | Product ObjectID |
| `quantity` | — | Defaults to `1` |

**Response** `200 OK` — returns updated cart object.

---

### `PUT /api/cart/items/:productId` 🔒

Set the quantity of a specific product in the cart (replaces existing quantity).

**Request**
```json
{ "quantity": 3 }
```

**Response** `200 OK` — returns updated cart object.

---

### `DELETE /api/cart/items/:productId` 🔒

Remove a product from the cart.

**Response** `200 OK` — returns updated cart object.

---

### `DELETE /api/cart` 🔒

Clear all items from the cart.

**Response** `200 OK`
```json
{ "message": "cart cleared" }
```

---

## Reviews

### `GET /api/products/:id/reviews`

Get all reviews for a product. Public — no auth required.

**Response** `200 OK`
```json
{
  "data": [
    {
      "id": "64a1b2c3d4e5f6a7b8c9d0e2",
      "userId": "64a1b2c3d4e5f6a7b8c9d0e0",
      "productId": "64a1b2c3d4e5f6a7b8c9d0e1",
      "rating": 5,
      "comment": "Отличная книга!",
      "createdAt": "2024-01-15T09:30:00Z",
      "updatedAt": "2024-01-15T09:30:00Z"
    }
  ],
  "averageRating": 4.7,
  "reviewCount": 23
}
```

---

### `POST /api/products/:id/reviews` 🔒

Submit a review. One review per user per product — returns `409` if already reviewed.

**Request**
```json
{ "rating": 5, "comment": "Отличная книга!" }
```

| Field | Required | Constraint |
|-------|----------|------------|
| `rating` | ✅ | Integer 1–5 |
| `comment` | — | — |

**Response** `201 Created` — returns the created review object.

---

### `PUT /api/reviews/:id` 🔒

Update your own review.

**Request**
```json
{ "rating": 4, "comment": "Хорошая книга" }
```

**Response** `200 OK` — returns updated review object.

---

### `DELETE /api/reviews/:id` 🔒

Delete your own review.

**Response** `200 OK`
```json
{ "message": "review deleted" }
```

---

## My Books (Library)

### `GET /api/users/me/books` 🔒

Returns all books in the user's library (purchased via orders).

```json
{
  "data": [
    {
      "id": "64a1b2c3d4e5f6a7b8c9d0e3",
      "userId": "64a1b2c3d4e5f6a7b8c9d0e0",
      "productId": "64a1b2c3d4e5f6a7b8c9d0e1",
      "purchasedAt": "2024-01-15T09:30:00Z"
    }
  ]
}
```

---

### `DELETE /api/users/me/books/:productId` 🔒

Remove a book from the user's library.

**Response** `200 OK`
```json
{ "message": "book removed from library" }
```

---

## Franchises (Branch Locations)

### `GET /api/franchises`

List all active store locations. Public.

**Query Parameters**

| Parameter | Description |
|-----------|-------------|
| `city` | Filter by city name (e.g. `Ташкент`) |
| `district` | Filter by district name |

**Response** `200 OK`
```json
{
  "data": [
    {
      "id": "64a1b2c3d4e5f6a7b8c9d0e4",
      "name": "Кадышева рынок",
      "description": "Главный магазин",
      "image": "https://klasstovar.uz/uploads/franchises/store.jpg",
      "city": "Ташкент",
      "district": "Яланглосский",
      "address": "ул. Кадышева, 12",
      "phone": "+998712345678",
      "weekdays": { "open": "09:00", "close": "18:00" },
      "weekend": { "open": "10:00", "close": "16:00" },
      "weekendClosed": false,
      "latitude": 41.2995,
      "longitude": 69.2401,
      "isActive": true,
      "createdAt": "2024-01-15T09:30:00Z",
      "updatedAt": "2024-01-15T09:30:00Z"
    }
  ]
}
```

---

### `GET /api/franchises/cities`

Returns list of distinct city names that have active franchises.

```json
{ "data": ["Ташкент", "Самарканд", "Бухара"] }
```

---

### `GET /api/franchises/:id`

Get a single franchise by ID.

**Response** `200 OK` — returns franchise object.

---

## Gift Certificates

### `GET /api/gift-certificates`

List all active gift certificate options. Public.

```json
{
  "data": [
    {
      "id": "64a1b2c3d4e5f6a7b8c9d0e5",
      "amount": 50000,
      "description": "Подарочный сертификат на 50 000 сум",
      "image": "https://klasstovar.uz/uploads/certificates/50k.jpg",
      "isActive": true,
      "sortOrder": 1,
      "createdAt": "2024-01-15T09:30:00Z",
      "updatedAt": "2024-01-15T09:30:00Z"
    }
  ]
}
```

---

### `GET /api/gift-certificates/:id`

Get a single gift certificate option by ID.

**Response** `200 OK` — returns gift certificate object.

---

## Blog

### `GET /api/blog/categories`

List all blog categories. Public.

```json
{
  "data": [
    {
      "id": "64a1b2c3d4e5f6a7b8c9d0e6",
      "name": "Новости",
      "slug": "novosti",
      "createdAt": "2024-01-15T09:30:00Z",
      "updatedAt": "2024-01-15T09:30:00Z"
    }
  ]
}
```

---

### `GET /api/blog/posts`

List published blog posts (paginated). Public.

**Query Parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `page` | `1` | Page number |
| `limit` | `12` | Items per page (max `100`) |
| `category` | — | Filter by blog category ObjectID |

**Response** `200 OK`
```json
{
  "data": [
    {
      "id": "64a1b2c3d4e5f6a7b8c9d0e7",
      "title": "10 лучших книг этого лета",
      "content": "...",
      "excerpt": "Краткое описание статьи...",
      "featuredImage": "https://klasstovar.uz/uploads/blog/summer.jpg",
      "categoryId": "64a1b2c3d4e5f6a7b8c9d0e6",
      "authorName": "Редакция Topar",
      "isPublished": true,
      "viewCount": 1240,
      "createdAt": "2024-06-07T10:00:00Z",
      "updatedAt": "2024-06-07T10:00:00Z"
    }
  ],
  "total": 42,
  "page": 1,
  "limit": 12
}
```

---

### `GET /api/blog/posts/popular`

List the most-viewed published posts. Public.

**Query Parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `limit` | `5` | Number of posts (max `20`) |

**Response** `200 OK` — same format as `GET /api/blog/posts` but without `content` field and no pagination.

---

### `GET /api/blog/posts/:id`

Get a full blog post by ID. Increments `viewCount` automatically.

**Response** `200 OK` — returns the full post object including `content`.

---

## Images

Static files are served directly from the server.

**Pattern:** `https://klasstovar.uz/uploads/main-products/<filename>`

The `coverUrl` and `covers` fields in product responses already contain full absolute URLs.

---

## Pagination

List responses that support pagination include a `pagination` or flat pagination fields:

```json
{ "data": [...], "total": 42, "page": 1, "limit": 20 }
```

To fetch the next page: add `?page=2` to the request.

---

## Usage Examples

```
# Register a new user
POST /api/auth/register
Content-Type: application/json
{"email":"user@example.com","password":"secret123","firstName":"Адил"}

# Login
POST /api/auth/login
Content-Type: application/json
{"email":"user@example.com","password":"secret123"}

# Get categories
GET /categories

# Search products
GET /mainProducts?search=гарри+поттер&page=1&limit=20

# Get products in a category
GET /mainProducts?categoryId=64a1b2c3d4e5f6a7b8c9d0e2&page=1&limit=50

# Add item to cart
POST /api/cart/items
Authorization: Bearer <token>
Content-Type: application/json
{"productId":"64a1b2c3d4e5f6a7b8c9d0e1","quantity":1}

# Place an order
POST /api/orders
Authorization: Bearer <token>
Content-Type: application/json
{"paymentMethod":"card","deliveryAmount":50000}

# Get reviews for a product
GET /api/products/64a1b2c3d4e5f6a7b8c9d0e1/reviews

# Get franchises in Tashkent
GET /api/franchises?city=Ташкент

# Get blog posts
GET /api/blog/posts?page=1&limit=12
```
