"""HTML view for the full stack DBDuck showcase app."""

HOME_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DBDuck Storefront</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #08101f;
      --card: rgba(15, 23, 42, 0.92);
      --line: rgba(148, 163, 184, 0.16);
      --text: #f8fafc;
      --muted: #94a3b8;
      --brand: #1a73e8;
      --brand-soft: rgba(26, 115, 232, 0.14);
      --success: #22c55e;
      --danger: #f87171;
      --shadow: 0 18px 54px rgba(8, 16, 31, 0.45);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, system-ui, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(26, 115, 232, 0.16), transparent 32%),
        radial-gradient(circle at top right, rgba(34, 197, 94, 0.12), transparent 28%),
        var(--bg);
      color: var(--text);
    }
    .shell { max-width: 1260px; margin: 0 auto; padding: 28px 20px 72px; }
    .topbar, .panel, .hero-card, .summary-card, .product-card, .cart-card, .order-card, .stat {
      border: 1px solid var(--line);
      background: var(--card);
      border-radius: 24px;
      box-shadow: var(--shadow);
    }
    .topbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      padding: 14px 18px;
      margin-bottom: 22px;
      background: rgba(8, 16, 31, 0.72);
      backdrop-filter: blur(18px);
    }
    .brand { display: flex; align-items: center; gap: 12px; }
    .brand-mark {
      width: 42px;
      height: 42px;
      border-radius: 14px;
      overflow: hidden;
      border: 1px solid rgba(30, 212, 228, 0.24);
      background: linear-gradient(180deg, rgba(26, 115, 232, 0.22), rgba(26, 115, 232, 0.08));
    }
    .brand-mark img { width: 100%; height: 100%; object-fit: cover; }
    .brand-copy strong { display: block; }
    .brand-copy span { color: var(--muted); font-size: 0.92rem; }
    .topbar-actions { display: flex; flex-wrap: wrap; gap: 10px; }
    .pill {
      padding: 9px 13px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(15, 23, 42, 0.9);
      color: var(--muted);
      font-size: 0.92rem;
    }
    .hero {
      display: grid;
      grid-template-columns: minmax(0, 1.15fr) minmax(320px, 0.85fr);
      gap: 22px;
      margin-bottom: 20px;
    }
    .hero-card { padding: 32px; }
    .hero h1 { margin: 0; font-size: clamp(2.3rem, 5vw, 4.5rem); line-height: 1.02; letter-spacing: -0.04em; }
    .hero p { color: var(--muted); line-height: 1.75; max-width: 720px; }
    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 8px 14px;
      border-radius: 999px;
      background: rgba(34, 197, 94, 0.12);
      border: 1px solid rgba(34, 197, 94, 0.18);
      color: #bbf7d0;
      font-size: 0.9rem;
      margin-bottom: 20px;
    }
    .eyebrow::before {
      content: "";
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: var(--success);
      box-shadow: 0 0 0 8px rgba(34, 197, 94, 0.14);
    }
    .hero-actions { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 24px; }
    .button, button {
      appearance: none;
      border: 0;
      border-radius: 14px;
      padding: 12px 16px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      transition: transform 0.18s ease;
    }
    .button:hover, button:hover { transform: translateY(-1px); }
    .button.primary, button.primary, button { background: var(--brand); color: white; }
    .button.secondary, button.secondary { background: rgba(15, 23, 42, 0.88); color: var(--text); border: 1px solid var(--line); }
    .summary-card { padding: 24px; display: grid; gap: 14px; }
    .summary-block {
      padding: 18px;
      border-radius: 18px;
      background: rgba(8, 16, 31, 0.58);
      border: 1px solid var(--line);
    }
    .summary-block span { color: var(--muted); font-size: 0.92rem; }
    .summary-block strong { display: block; margin-top: 8px; font-size: 1.4rem; }
    .status {
      margin: 0 0 18px;
      padding: 14px 16px;
      border-radius: 16px;
      border: 1px solid var(--line);
      display: none;
    }
    .status.error { display: block; border-color: rgba(248, 113, 113, 0.4); background: rgba(127, 29, 29, 0.18); color: #fecaca; }
    .status.success { display: block; border-color: rgba(74, 222, 128, 0.35); background: rgba(20, 83, 45, 0.18); color: #bbf7d0; }
    .toast {
      position: fixed;
      top: 22px;
      right: 22px;
      z-index: 9999;
      min-width: 300px;
      max-width: min(420px, calc(100vw - 28px));
      padding: 16px 18px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(8, 16, 31, 0.96);
      box-shadow: 0 24px 64px rgba(8, 16, 31, 0.55);
      transform: translateY(-14px);
      opacity: 0;
      pointer-events: none;
      transition: opacity 0.2s ease, transform 0.2s ease;
    }
    .toast.show {
      opacity: 1;
      transform: translateY(0);
      pointer-events: auto;
    }
    .toast.error { border-color: rgba(248, 113, 113, 0.45); background: rgba(69, 10, 10, 0.96); color: #fecaca; }
    .toast.success { border-color: rgba(74, 222, 128, 0.4); background: rgba(20, 83, 45, 0.95); color: #dcfce7; }
    .toast-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 6px;
    }
    .toast-title { font-weight: 800; font-size: 0.95rem; }
    .toast-close {
      background: transparent;
      color: inherit;
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 10px;
      padding: 6px 8px;
      line-height: 1;
    }
    .toast-close:hover { transform: none; opacity: 0.9; }
    .toast-message { color: inherit; line-height: 1.55; font-size: 0.94rem; }
    .stats-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 24px;
    }
    .stat { padding: 20px; }
    .stat-label { color: var(--muted); font-size: 0.92rem; }
    .stat-value { display: block; margin-top: 8px; font-size: 1.9rem; font-weight: 800; letter-spacing: -0.04em; }
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1.6fr) minmax(320px, 0.8fr);
      gap: 22px;
    }
    .stack { display: grid; gap: 22px; }
    .panel, .cart-card { padding: 24px; }
    .section-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 18px; }
    .section-head p { margin: 6px 0 0; color: var(--muted); }
    .auth-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }
    form { display: grid; gap: 12px; }
    input, select {
      width: 100%;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(8, 16, 31, 0.85);
      color: var(--text);
      padding: 12px 14px;
      font: inherit;
    }
    input::placeholder { color: #64748b; }
    .helper { color: var(--muted); font-size: 0.9rem; line-height: 1.6; }
    .auth-note, .mini-card {
      padding: 14px 16px;
      border-radius: 16px;
      background: rgba(8, 16, 31, 0.58);
      border: 1px solid var(--line);
    }
    .products-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; }
    .product-card { padding: 18px; display: grid; gap: 14px; }
    .product-top { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }
    .product-price { font-size: 1.35rem; font-weight: 800; }
    .tag {
      display: inline-flex;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--brand-soft);
      color: #bfdbfe;
      font-size: 0.82rem;
      border: 1px solid rgba(26, 115, 232, 0.24);
    }
    .product-actions { display: grid; grid-template-columns: 92px 1fr; gap: 10px; }
    .cart-card { position: sticky; top: 24px; display: grid; gap: 18px; }
    .cart-list, .order-list { display: grid; gap: 12px; }
    .empty {
      padding: 18px;
      border-radius: 16px;
      border: 1px dashed var(--line);
      color: var(--muted);
      background: rgba(8, 16, 31, 0.45);
      text-align: center;
    }
    .cart-item, .order-card { padding: 14px 16px; }
    .cart-item-row, .order-top { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .quantity-controls { display: inline-flex; align-items: center; gap: 8px; }
    .quantity-controls button {
      width: 32px;
      height: 32px;
      padding: 0;
      border-radius: 10px;
      background: rgba(15, 23, 42, 0.82);
      border: 1px solid var(--line);
      color: var(--text);
    }
    .cart-summary {
      padding-top: 10px;
      border-top: 1px solid var(--line);
      display: grid;
      gap: 10px;
    }
    .summary-line { display: flex; align-items: center; justify-content: space-between; color: var(--muted); }
    .summary-line.total { color: var(--text); font-size: 1.04rem; font-weight: 800; }
    .order-card { background: rgba(8, 16, 31, 0.6); border-radius: 18px; border: 1px solid var(--line); }
    .order-meta { color: var(--muted); font-size: 0.9rem; }
    .paid { color: #bbf7d0; }
    .pending { color: #fde68a; }
    .hidden { display: none !important; }
    @media (max-width: 1100px) {
      .hero, .layout, .auth-grid { grid-template-columns: 1fr; }
      .cart-card { position: static; }
      .stats-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 720px) {
      .topbar { flex-direction: column; align-items: stretch; }
      .topbar-actions { justify-content: flex-start; }
      .stats-grid { grid-template-columns: 1fr; }
      .products-grid { grid-template-columns: 1fr; }
      .shell { padding: 18px 14px 48px; }
      .hero-card, .panel, .cart-card, .summary-card { padding: 20px; }
    }
  </style>
</head>
<body>
  <div id="toast" class="toast" role="alert" aria-live="assertive">
    <div class="toast-head">
      <span id="toast-title" class="toast-title">Notice</span>
      <button class="toast-close" type="button" onclick="hideToast()">Close</button>
    </div>
    <div id="toast-message" class="toast-message"></div>
  </div>
  <div class="shell">
    <header class="topbar">
      <div class="brand">
        <div class="brand-mark">
          <img src="/dbduck-logo-transparent-clean.png" alt="DBDuck" />
        </div>
        <div class="brand-copy">
          <strong>DBDuck Store</strong>
          <span>Customer storefront with login, signup, checkout, and admin inventory control.</span>
        </div>
      </div>
      <div class="topbar-actions">
        <span class="pill">Secure password hashing</span>
        <span class="pill">Session-based auth</span>
        <span class="pill">Admin inventory panel</span>
      </div>
    </header>

    <section class="hero">
      <div class="hero-card">
        <div class="eyebrow">Full web application</div>
        <h1>Sign in, shop, and manage inventory in one DBDuck app.</h1>
        <p>
          This example now behaves like a real product application instead of a developer console. End users can sign up,
          log in, browse the catalog, and place orders. Admin users get inventory tools and operational visibility.
        </p>
        <div class="hero-actions">
          <a class="button primary" href="#auth">Open account area</a>
          <a class="button secondary" href="#shop">Browse catalog</a>
        </div>
      </div>
      <aside class="summary-card">
        <div class="summary-block">
          <span>Current user</span>
          <strong id="current-user-name">Guest</strong>
        </div>
        <div class="summary-block">
          <span>Role</span>
          <strong id="current-user-role">Visitor</strong>
        </div>
        <div class="summary-block">
          <span>Cart total</span>
          <strong id="hero-cart-total">Rs. 0.00</strong>
        </div>
        <div class="summary-block">
          <span>Demo admin</span>
          <strong>admin@dbduck.app / admin123</strong>
        </div>
      </aside>
    </section>

    <div id="status-banner" class="status" role="status" aria-live="polite"></div>

    <section class="stats-grid" id="stats"></section>

    <div class="layout">
      <div class="stack">
        <section class="panel" id="auth">
          <div class="section-head">
            <div>
              <h2>Account access</h2>
              <p>Customers can sign up or log in. Admin accounts unlock product management.</p>
            </div>
            <button id="logout-button" class="secondary hidden" type="button" onclick="logout()">Log out</button>
          </div>
          <div class="auth-note" id="auth-state-note">You are browsing as a guest. Sign in to place orders.</div>
          <div class="auth-grid">
            <form id="signup-form">
              <h3>Create account</h3>
              <input name="name" placeholder="Full name" required />
              <input name="email" type="email" placeholder="Email address" required />
              <input name="password" type="password" placeholder="Create a password" required />
              <input name="bio" placeholder="Short bio" />
              <button type="submit">Sign up</button>
            </form>
            <form id="login-form">
              <h3>Log in</h3>
              <input name="email" type="email" placeholder="Email address" required />
              <input name="password" type="password" placeholder="Password" required />
              <button type="submit">Log in</button>
              <div class="helper">Use the seeded admin account for inventory management and dashboard controls.</div>
            </form>
          </div>
        </section>

        <section class="panel hidden" id="admin-panel">
          <div class="section-head">
            <div>
              <h2>Admin inventory panel</h2>
              <p>Create products directly from the admin account.</p>
            </div>
          </div>
          <form id="product-form">
            <input name="name" placeholder="Product name" required />
            <input name="price" placeholder="Price" type="number" min="1" step="0.01" required />
            <button type="submit">Add product</button>
          </form>
        </section>

        <section class="panel" id="shop">
          <div class="section-head">
            <div>
              <h2>Catalog</h2>
              <p>Browse live products and add them to the cart.</p>
            </div>
            <button class="secondary" type="button" onclick="loadProducts()">Refresh catalog</button>
          </div>
          <div class="products-grid" id="product-grid"></div>
        </section>

        <section class="panel" id="orders">
          <div class="section-head">
            <div>
              <h2>Recent orders</h2>
              <p>Customers see their orders. Admins see everything.</p>
            </div>
            <button class="secondary" type="button" onclick="loadOrders()">Refresh orders</button>
          </div>
          <div class="order-list" id="orders-list"></div>
        </section>
      </div>

      <aside class="cart-card">
        <div>
          <h2>Your cart</h2>
          <p class="helper">Checkout is available once you are logged in. Orders are placed against the active account only through the payment gateway.</p>
        </div>
        <div class="mini-card">
          <strong id="cart-user-label">Guest session</strong>
          <div class="helper" id="cart-user-helper">Log in or create an account to continue.</div>
        </div>
        <div class="cart-list" id="cart-list"></div>
        <div class="cart-summary">
          <div class="summary-line"><span>Items</span><span id="cart-count">0</span></div>
          <div class="summary-line"><span>Status</span><span id="checkout-status">Login required</span></div>
          <div class="summary-line total"><span>Total</span><span id="cart-total">Rs. 0.00</span></div>
        </div>
        <label>
          Order tags
          <input id="order-tags" placeholder="priority,gift" />
        </label>
        <button id="checkout-button" type="button" onclick="checkout()">Pay securely</button>
      </aside>
    </div>
  </div>

  <script>
    const state = {
      currentUser: null,
      products: [],
      orders: [],
      cart: [],
      paymentGatewayKey: "",
    };
    let toastTimer = null;

    async function api(path, options = {}) {
      const res = await fetch(path, {
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        ...options,
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
      return data;
    }

    function currency(value) {
      return `Rs. ${Number(value || 0).toFixed(2)}`;
    }

    function setStatus(message, kind = "success") {
      const el = document.getElementById("status-banner");
      el.className = `status ${kind}`;
      el.textContent = message;
      showToast(message, kind);
    }

    function clearStatus() {
      const el = document.getElementById("status-banner");
      el.className = "status";
      el.textContent = "";
    }

    function hideToast() {
      const toast = document.getElementById("toast");
      toast.className = "toast";
      if (toastTimer) {
        clearTimeout(toastTimer);
        toastTimer = null;
      }
    }

    function showToast(message, kind = "success") {
      const toast = document.getElementById("toast");
      document.getElementById("toast-title").textContent = kind === "error" ? "Error" : "Success";
      document.getElementById("toast-message").textContent = message;
      toast.className = `toast ${kind} show`;
      if (toastTimer) {
        clearTimeout(toastTimer);
      }
      toastTimer = setTimeout(() => {
        hideToast();
      }, kind === "error" ? 6000 : 3200);
    }

    function getCartTotal() {
      return state.cart.reduce((sum, item) => sum + item.price * item.quantity, 0);
    }

    function renderAuthState() {
      const user = state.currentUser;
      document.getElementById("current-user-name").textContent = user ? user.name : "Guest";
      document.getElementById("current-user-role").textContent = user ? user.role : "Visitor";
      document.getElementById("hero-cart-total").textContent = currency(getCartTotal());
      document.getElementById("logout-button").classList.toggle("hidden", !user);
      document.getElementById("admin-panel").classList.toggle("hidden", !(user && user.role === "admin"));
      document.getElementById("auth-state-note").textContent = user
        ? `Signed in as ${user.name} (${user.role}).`
        : "You are browsing as a guest. Sign in to place orders.";
      document.getElementById("cart-user-label").textContent = user ? user.name : "Guest session";
      document.getElementById("cart-user-helper").textContent = user
        ? `${user.email} | ${user.role}`
        : "Log in or create an account to continue.";
      document.getElementById("checkout-status").textContent = user ? "Ready" : "Login required";
    }

    function renderStats(data) {
      document.getElementById("stats").innerHTML = `
        <article class="stat">
          <span class="stat-label">Customers</span>
          <strong class="stat-value">${data.customers}</strong>
        </article>
        <article class="stat">
          <span class="stat-label">Products</span>
          <strong class="stat-value">${data.products}</strong>
        </article>
        <article class="stat">
          <span class="stat-label">Orders</span>
          <strong class="stat-value">${data.orders}</strong>
        </article>
        <article class="stat">
          <span class="stat-label">Completed</span>
          <strong class="stat-value">${data.completed_orders}</strong>
        </article>
      `;
    }

    function renderProducts() {
      const root = document.getElementById("product-grid");
      if (!state.products.length) {
        root.innerHTML = '<div class="empty">No products are available right now.</div>';
        return;
      }
      root.innerHTML = state.products.map((product) => `
        <article class="product-card">
          <div class="product-top">
            <div>
              <h3>${product.name}</h3>
              <div class="helper">Inventory item managed by DBDuck.</div>
            </div>
            <span class="tag">${product.active ? "Available" : "Hidden"}</span>
          </div>
          <div class="product-price">${currency(product.price)}</div>
          <div class="product-actions">
            <input id="qty-${product.id}" type="number" min="1" value="1" />
            <button type="button" onclick="addToCart(${product.id})">Add to cart</button>
          </div>
        </article>
      `).join("");
    }

    function renderCart() {
      const root = document.getElementById("cart-list");
      const itemCount = state.cart.reduce((sum, item) => sum + item.quantity, 0);
      document.getElementById("cart-count").textContent = String(itemCount);
      document.getElementById("cart-total").textContent = currency(getCartTotal());
      document.getElementById("hero-cart-total").textContent = currency(getCartTotal());
      if (!state.cart.length) {
        root.innerHTML = '<div class="empty">Your cart is empty.</div>';
        return;
      }
      root.innerHTML = state.cart.map((item) => `
        <div class="cart-item">
          <div class="cart-item-row">
            <div>
              <strong>${item.name}</strong>
              <div class="helper">${currency(item.price)} each</div>
            </div>
            <button class="secondary" type="button" onclick="removeFromCart(${item.id})">Remove</button>
          </div>
          <div class="cart-item-row" style="margin-top: 12px;">
            <div class="quantity-controls">
              <button type="button" onclick="changeQuantity(${item.id}, -1)">-</button>
              <strong>${item.quantity}</strong>
              <button type="button" onclick="changeQuantity(${item.id}, 1)">+</button>
            </div>
            <strong>${currency(item.quantity * item.price)}</strong>
          </div>
        </div>
      `).join("");
    }

    function renderOrders() {
      const root = document.getElementById("orders-list");
      if (!state.currentUser) {
        root.innerHTML = '<div class="empty">Log in to view your orders.</div>';
        return;
      }
      const visibleOrders = state.currentUser && state.currentUser.role !== "admin"
        ? state.orders.filter((order) => Number(order.customer_id) === Number(state.currentUser.id))
        : state.orders;
      if (!visibleOrders.length) {
        root.innerHTML = '<div class="empty">No orders to show yet.</div>';
        return;
      }
      root.innerHTML = visibleOrders.map((order) => `
        <article class="order-card">
          <div class="order-top">
            <div>
              <strong>Order #${order.id}</strong>
              <div class="order-meta">${order.customer_name || "Unknown customer"} | ${order.created_at || "No timestamp"}</div>
            </div>
            <div style="text-align: right;">
              <strong>${currency(order.total)}</strong>
              <div class="order-meta ${order.paid ? "paid" : "pending"}">${order.paid ? "Paid" : "Pending"}</div>
            </div>
          </div>
          <div class="helper" style="margin-top: 10px;">${order.items.map((item) => `${item.product_name} x ${item.quantity}`).join(" | ") || "No items"}</div>
        </article>
      `).join("");
    }

    function addToCart(productId) {
      const quantityInput = document.getElementById(`qty-${productId}`);
      const quantity = Math.max(1, Number(quantityInput ? quantityInput.value : 1) || 1);
      const product = state.products.find((item) => item.id === productId);
      if (!product) {
        setStatus("Product not found in the latest catalog.", "error");
        return;
      }
      const existing = state.cart.find((item) => item.id === productId);
      if (existing) {
        existing.quantity += quantity;
      } else {
        state.cart.push({ ...product, quantity });
      }
      setStatus(`${product.name} added to cart.`);
      renderCart();
    }

    function changeQuantity(productId, delta) {
      const item = state.cart.find((entry) => entry.id === productId);
      if (!item) return;
      item.quantity += delta;
      if (item.quantity <= 0) {
        state.cart = state.cart.filter((entry) => entry.id !== productId);
      }
      renderCart();
    }

    function removeFromCart(productId) {
      state.cart = state.cart.filter((entry) => entry.id !== productId);
      renderCart();
    }

    async function loadCurrentUser() {
      const data = await api("/api/auth/me");
      state.currentUser = data.user;
      renderAuthState();
      renderOrders();
    }

    async function loadDashboard() {
      const stats = await api("/api/dashboard");
      renderStats(stats);
    }

    async function loadProducts() {
      state.products = await api("/api/products");
      renderProducts();
    }

    async function loadPaymentConfig() {
      const config = await api("/api/payments/config");
      state.paymentGatewayKey = config.key_id || "";
      return config;
    }

    async function loadOrders() {
      state.orders = await api("/api/orders");
      renderOrders();
    }

    async function signup(event) {
      event.preventDefault();
      try {
        clearStatus();
        const payload = Object.fromEntries(new FormData(event.target).entries());
        const result = await api("/api/auth/signup", { method: "POST", body: JSON.stringify(payload) });
        state.currentUser = result.user;
        event.target.reset();
        renderAuthState();
        setStatus(`Welcome, ${result.user.name}. Your account is ready.`);
        await Promise.all([loadDashboard(), loadOrders()]);
      } catch (error) {
        setStatus(error.message, "error");
      }
    }

    async function login(event) {
      event.preventDefault();
      try {
        clearStatus();
        const payload = Object.fromEntries(new FormData(event.target).entries());
        const result = await api("/api/auth/login", { method: "POST", body: JSON.stringify(payload) });
        state.currentUser = result.user;
        event.target.reset();
        renderAuthState();
        setStatus(`Welcome back, ${result.user.name}.`);
        await Promise.all([loadDashboard(), loadOrders(), loadProducts()]);
      } catch (error) {
        setStatus(error.message, "error");
      }
    }

    async function logout() {
      try {
        clearStatus();
        await api("/api/auth/logout", { method: "POST" });
        state.currentUser = null;
        state.cart = [];
        state.orders = [];
        state.products = [];
        document.getElementById("order-tags").value = "";
        document.getElementById("signup-form").reset();
        document.getElementById("login-form").reset();
        document.getElementById("product-form").reset();
        renderAuthState();
        renderProducts();
        renderCart();
        renderOrders();
        window.location.hash = "auth";
        setStatus("You have been logged out.");
      } catch (error) {
        setStatus(error.message, "error");
      }
    }

    async function createProduct(event) {
      event.preventDefault();
      try {
        clearStatus();
        const payload = Object.fromEntries(new FormData(event.target).entries());
        payload.price = Number(payload.price);
        await api("/api/products", { method: "POST", body: JSON.stringify(payload) });
        event.target.reset();
        await Promise.all([loadProducts(), loadDashboard()]);
        setStatus("Product created successfully.");
      } catch (error) {
        setStatus(error.message, "error");
      }
    }

    async function ensurePaymentGatewayScript() {
      if (window.Razorpay) {
        return;
      }
      await new Promise((resolve, reject) => {
        const existing = document.querySelector('script[data-payment-gateway="checkout"]');
        if (existing) {
          existing.addEventListener("load", resolve, { once: true });
          existing.addEventListener("error", () => reject(new Error("Unable to load Payment Gateway")), { once: true });
          return;
        }
        const script = document.createElement("script");
        script.src = "https://checkout.razorpay.com/v1/checkout.js";
        script.async = true;
        script.dataset.paymentGateway = "checkout";
        script.onload = resolve;
        script.onerror = () => reject(new Error("Unable to load Payment Gateway"));
        document.head.appendChild(script);
      });
    }

    async function checkout() {
      try {
        clearStatus();
        if (!state.currentUser) {
          throw new Error("Log in before placing an order.");
        }
        if (!state.cart.length) {
          throw new Error("Add at least one product to the cart before checkout.");
        }
        const config = await loadPaymentConfig();
        if (!config.configured || !config.key_id) {
          throw new Error("Payment Gateway is not configured on this server.");
        }
        await ensurePaymentGatewayScript();
        const tags = document.getElementById("order-tags").value
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean);
        const checkoutPayload = {
          items: state.cart.map((item) => ({ product_id: item.id, quantity: item.quantity })),
          tags,
          status: "completed",
        };
        const gateway = await api("/api/payments/checkout", {
          method: "POST",
          body: JSON.stringify(checkoutPayload),
        });
        const options = {
          key: gateway.key,
          amount: gateway.amount,
          currency: gateway.currency,
          name: "DBDuck Store",
          description: "Secure checkout",
          order_id: gateway.order_id,
          prefill: {
            name: gateway.customer.name,
            email: gateway.customer.email,
          },
          notes: {
            app: "DBDuck Store",
          },
          theme: {
            color: "#1a73e8",
          },
          handler: async function (response) {
            try {
              const completed = await api("/api/payments/complete", {
                method: "POST",
                body: JSON.stringify({
                  razorpay_order_id: response.razorpay_order_id,
                  razorpay_payment_id: response.razorpay_payment_id,
                  razorpay_signature: response.razorpay_signature,
                  items: checkoutPayload.items,
                  tags: checkoutPayload.tags,
                  status: checkoutPayload.status,
                }),
              });
              state.cart = [];
              document.getElementById("order-tags").value = "";
              renderCart();
              await Promise.all([loadDashboard(), loadOrders()]);
              setStatus(`Order #${completed.order.id} placed successfully.`);
            } catch (error) {
              setStatus(error.message, "error");
            }
          },
          modal: {
            ondismiss: function () {
              setStatus("Payment cancelled.", "error");
            },
          },
        };
        const gatewayCheckout = new window.Razorpay(options);
        gatewayCheckout.open();
      } catch (error) {
        setStatus(error.message, "error");
      }
    }

    document.getElementById("signup-form").addEventListener("submit", signup);
    document.getElementById("login-form").addEventListener("submit", login);
    document.getElementById("product-form").addEventListener("submit", createProduct);

    Promise.all([loadCurrentUser(), loadDashboard(), loadProducts(), loadOrders(), loadPaymentConfig()]).then(() => {
      renderCart();
    }).catch((error) => {
      setStatus(error.message, "error");
    });
  </script>
</body>
</html>
"""
