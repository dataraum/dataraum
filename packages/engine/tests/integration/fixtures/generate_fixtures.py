#!/usr/bin/env python3
"""Generate synthetic test fixtures for integration tests.

Creates realistic-looking finance data with enough rows for meaningful
statistical analysis (100+ rows per table).

Run this script to regenerate fixtures:
    python tests/integration/fixtures/generate_fixtures.py
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Seed for reproducibility
random.seed(42)

FIXTURES_DIR = Path(__file__).parent / "small_finance"
NUM_CUSTOMERS = 100
NUM_VENDORS = 50
NUM_PRODUCTS = 30
NUM_TRANSACTIONS = 500

# Realistic data pools
FIRST_NAMES = [
    "James",
    "Mary",
    "John",
    "Patricia",
    "Robert",
    "Jennifer",
    "Michael",
    "Linda",
    "William",
    "Elizabeth",
    "David",
    "Barbara",
    "Richard",
    "Susan",
    "Joseph",
    "Jessica",
    "Thomas",
    "Sarah",
    "Charles",
    "Karen",
    "Daniel",
    "Nancy",
    "Matthew",
    "Lisa",
    "Anthony",
    "Betty",
    "Mark",
    "Margaret",
    "Donald",
    "Sandra",
    "Steven",
    "Ashley",
]

LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Ramirez",
    "Lewis",
    "Robinson",
    "Walker",
]

STREET_TYPES = ["Street", "Avenue", "Drive", "Road", "Lane", "Boulevard", "Way", "Court"]
STREET_NAMES = [
    "Main",
    "Oak",
    "Pine",
    "Maple",
    "Cedar",
    "Elm",
    "Washington",
    "Park",
    "Lake",
    "Hill",
    "River",
    "Spring",
    "Valley",
    "Forest",
    "Meadow",
    "Sunset",
]

CITIES = [
    ("Springfield", "IL"),
    ("Austin", "TX"),
    ("Seattle", "WA"),
    ("Denver", "CO"),
    ("Miami", "FL"),
    ("Boston", "MA"),
    ("Phoenix", "AZ"),
    ("Atlanta", "GA"),
    ("San Diego", "CA"),
    ("Minneapolis", "MN"),
    ("Portland", "OR"),
    ("Chicago", "IL"),
    ("Houston", "TX"),
    ("Dallas", "TX"),
    ("San Francisco", "CA"),
    ("New York", "NY"),
]

COMPANY_PREFIXES = [
    "Acme",
    "Global",
    "Premier",
    "Quality",
    "Standard",
    "Professional",
    "Advanced",
    "Elite",
    "Superior",
    "Prime",
    "Pacific",
    "Atlantic",
    "National",
    "United",
]

COMPANY_SUFFIXES = [
    "Supplies",
    "Services",
    "Solutions",
    "Partners",
    "Industries",
    "Consulting",
    "Technologies",
    "Systems",
    "Group",
    "Corporation",
    "Enterprises",
    "Associates",
]

COMPANY_TYPES = ["Inc", "LLC", "Corp", "Ltd", "Co"]

PRODUCT_NAMES = [
    "Consulting Services",
    "Software License",
    "Hardware Support",
    "Cloud Storage",
    "Data Analytics",
    "Technical Support",
    "Training Workshop",
    "Security Audit",
    "Network Setup",
    "Database Management",
    "API Integration",
    "Mobile Development",
    "Web Hosting",
    "Email Services",
    "Backup Solutions",
    "IT Maintenance",
    "Project Management",
    "Quality Assurance",
    "System Integration",
    "Help Desk",
    "Office Supplies",
    "Marketing Materials",
    "Print Services",
    "Design Services",
    "Legal Services",
    "Accounting Services",
    "HR Consulting",
    "Payroll Processing",
]

TRANSACTION_TYPES = [
    "Invoice",
    "Bill",
    "Payment",
    "Credit Card Credit",
    "Expense",
    "Refund",
    "Transfer",
]
ACCOUNTS = [
    "Accounts Receivable (A/R)",
    "Accounts Payable (A/P)",
    "Bank Account",
    "Credit Card",
    "Expenses",
    "Revenue",
    "Cost of Goods Sold",
]
PAYMENT_METHODS = [
    "Visa",
    "Mastercard",
    "American Express",
    "Check",
    "Wire Transfer",
    "ACH",
    "Cash",
]


def random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_address():
    num = random.randint(100, 9999)
    street = random.choice(STREET_NAMES)
    street_type = random.choice(STREET_TYPES)
    return f"{num} {street} {street_type}"


def random_city_state_zip():
    city, state = random.choice(CITIES)
    zip_code = str(random.randint(10000, 99999))
    return city, state, zip_code


def random_company():
    prefix = random.choice(COMPANY_PREFIXES)
    suffix = random.choice(COMPANY_SUFFIXES)
    company_type = random.choice(COMPANY_TYPES)
    return f"{prefix} {suffix} {company_type}"


def random_balance():
    if random.random() < 0.4:  # 40% chance of no balance
        return "--"
    return f"{random.uniform(50, 5000):.2f}"


def random_date(start_year=2023, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")


def generate_customers():
    """Generate customer fixture data."""
    rows = []
    for _ in range(NUM_CUSTOMERS):
        name = random_name()
        bill_city, bill_state, bill_zip = random_city_state_zip()
        ship_city, ship_state, ship_zip = random_city_state_zip()
        business_id = random.randint(1, 3)

        rows.append(
            {
                "Business Id": business_id,
                "Customer name": name,
                "Customer full name": name,
                "Billing address": random_address(),
                "Billing city": bill_city,
                "Billing state": bill_state,
                "Billing ZIP code": bill_zip,
                "Shipping address": random_address(),
                "Shipping city": ship_city,
                "Shipping state": ship_state,
                "Shipping ZIP code": ship_zip,
                "Balance": random_balance(),
            }
        )
    return rows


def generate_vendors():
    """Generate vendor fixture data."""
    rows = []
    for _ in range(NUM_VENDORS):
        city, state, zip_code = random_city_state_zip()
        business_id = random.randint(1, 3)

        rows.append(
            {
                "Business Id": business_id,
                "Vendor name": random_company(),
                "Billing address": random_address(),
                "Billing city": city,
                "Billing state": state,
                "Billing ZIP code": zip_code,
                "Balance": random_balance(),
            }
        )
    return rows


def generate_products():
    """Generate product/service fixture data."""
    rows = []
    used_names = set()

    for _ in range(NUM_PRODUCTS):
        # Ensure unique product names
        name = random.choice(PRODUCT_NAMES)
        while name in used_names:
            name = random.choice(PRODUCT_NAMES)
        used_names.add(name)

        business_id = random.randint(1, 3)
        product_type = "Service" if random.random() < 0.7 else "Product"

        rows.append(
            {
                "Business Id": business_id,
                "Product_Service": name,
                "Product_Service_Type": product_type,
            }
        )
    return rows


def generate_payment_methods():
    """Generate payment method fixture data."""
    rows = []
    methods = [
        ("Visa", "Yes"),
        ("Mastercard", "Yes"),
        ("American Express", "Yes"),
        ("Discover", "Yes"),
        ("Check", "No"),
        ("Wire Transfer", "No"),
        ("ACH", "No"),
        ("Cash", "No"),
        ("PayPal", "No"),
        ("Debit Card", "Yes"),
    ]

    for i, (method, is_credit) in enumerate(methods):
        business_id = (i % 3) + 1
        rows.append(
            {
                "Business Id": business_id,
                "Payment method": method,
                "Credit card": is_credit,
            }
        )
    return rows


def generate_transactions(customers, vendors, products):
    """Generate transaction fixture data."""
    customer_names = [c["Customer name"] for c in customers]
    vendor_names = [v["Vendor name"] for v in vendors]
    product_names = [p["Product_Service"] for p in products]

    rows = []
    for i in range(NUM_TRANSACTIONS):
        txn_type = random.choice(TRANSACTION_TYPES)
        business_id = random.randint(1, 3)
        txn_date = random_date()
        created_date = txn_date  # Usually same day

        # Amount varies by transaction type
        if txn_type in ["Invoice", "Bill"]:
            amount = random.uniform(500, 10000)
        elif txn_type == "Payment":
            amount = random.uniform(100, 5000)
        else:
            amount = random.uniform(50, 2000)

        # Customer or vendor based on type
        customer_name = "--"
        vendor_name = "--"
        if txn_type in ["Invoice", "Payment", "Credit Card Credit", "Refund"]:
            customer_name = random.choice(customer_names)
        elif txn_type in ["Bill", "Expense"]:
            vendor_name = random.choice(vendor_names)

        # Product/service
        product = random.choice(product_names) if random.random() < 0.7 else "--"
        quantity = random.randint(1, 50) if product != "--" else "--"
        rate = f"{random.uniform(10, 500):.2f}" if product != "--" else "--"

        # Credit/Debit based on type
        credit = (
            f"{amount:.2f}" if txn_type in ["Invoice", "Payment", "Credit Card Credit"] else "--"
        )
        debit = f"{amount:.2f}" if txn_type in ["Bill", "Expense"] else "--"

        # Status fields
        ar_paid = random.choice(["Paid", "Unpaid", "--"])
        ap_paid = random.choice(["Paid", "Unpaid", "--"])
        due_date = random_date() if txn_type in ["Invoice", "Bill"] else "--"
        open_balance = (
            f"{random.uniform(0, amount):.2f}"
            if ar_paid == "Unpaid" or ap_paid == "Unpaid"
            else "0.00"
        )

        rows.append(
            {
                "Business Id": business_id,
                "Transaction ID": 1000 + i,
                "Transaction date": txn_date,
                "Transaction type": txn_type,
                "Amount": f"{amount:.2f}",
                "Created date": created_date,
                "Created user": random.choice(["Admin", "Manager", "Staff", "Accountant"]),
                "Account": random.choice(ACCOUNTS),
                "A/R paid": ar_paid,
                "A/P paid": ap_paid,
                "Due date": due_date,
                "Open balance": open_balance,
                "PO status": random.choice(["--", "Open", "Closed"]),
                "Estimate status": "--",
                "Customer name": customer_name,
                "Vendor name": vendor_name,
                "Product_Service": product,
                "Quantity": quantity,
                "Rate": rate,
                "Credit": credit,
                "Debit": debit,
                "Sale": "Yes" if txn_type == "Invoice" else "No",
                "Purchase": "Yes" if txn_type == "Bill" else "No",
                "Billable": random.choice(["Yes", "No"]),
                "Invoiced": random.choice(["Yes", "No"]),
                "Cleared": random.choice(["Yes", "No"]),
                "Payment method": random.choice(PAYMENT_METHODS),
            }
        )
    return rows


def write_csv(filename: str, rows: list[dict]):
    """Write rows to CSV file."""
    if not rows:
        return

    filepath = FIXTURES_DIR / filename
    fieldnames = list(rows[0].keys())

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  {filename}: {len(rows)} rows, {len(fieldnames)} columns")


def main():
    """Generate all fixture files."""
    print("Generating synthetic test fixtures...")
    print(f"Output directory: {FIXTURES_DIR}")
    print()

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    # Generate data (some depend on others for foreign key references)
    customers = generate_customers()
    vendors = generate_vendors()
    products = generate_products()
    payment_methods = generate_payment_methods()
    transactions = generate_transactions(customers, vendors, products)

    # Write files
    write_csv("customers.csv", customers)
    write_csv("vendors.csv", vendors)
    write_csv("products.csv", products)
    write_csv("payment_methods.csv", payment_methods)
    write_csv("transactions.csv", transactions)

    print()
    print("Done! Fixtures generated with clean columns (no export noise).")


if __name__ == "__main__":
    main()
