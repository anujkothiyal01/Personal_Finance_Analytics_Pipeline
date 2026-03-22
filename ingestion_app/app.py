from flask import Flask, render_template, request, redirect, url_for, flash
from dotenv import load_dotenv
from datetime import date
from decimal import Decimal, InvalidOperation
import os
import snowflake.connector

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key")

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}


def get_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
        role=SNOWFLAKE_CONFIG["role"],
    )


CATEGORY_DATA = {
    "food": {
        "groceries": ["Blinkit", "BigBasket", "D-Mart"],
        "fast_food": ["Domino's", "Pizza Hut", "Burger King", "KFC"],
        "restaurant": ["Swiggy", "Zomato"],
        "tea_snacks": ["Cafe", "Bakery"],
    },
    "travel": {
        "rapido": ["Rapido"],
        "uber": ["Uber"],
        "ola": ["Ola"],
    },
    "bills": {
        "electricity": ["UPCL"],
    },
    "income": {
        "salary": ["CloudEQS"],
    },
    "transfer": {
        "wallet_transfer": ["Paytm Wallet"],
    },
    "utilities": {
        "wifi_recharge": ["JioFiber"],
        "mobile_recharge": ["Airtel"],
    },
    "health": {
        "gym": ["Anytime Fitness"],
    },
    "entertainment": {
        "movies": ["BookMyShow"],
        "netflix_subscription": ["Netflix"],
    },
    "investment": {
        "sip": ["Paytm Money"],
    },
    "shopping": {
        "personal_care": ["Apollo", "Nykaa", "Local Store"],
        "clothing": ["Myntra", "Amazon"],
    },
}

TXN_TYPE_ALLOWED_CATEGORIES = {
    "expense": [
        "food",
        "travel",
        "bills",
        "utilities",
        "health",
        "entertainment",
        "investment",
        "shopping",
    ],
    "income": ["income"],
    "transfer": ["transfer"],
}

PAYMENT_MODES = ["upi", "cash", "card", "bank_transfer"]
RECORD_STATUS_OPTIONS = ["Active", "Inactive", "Deleted"]
RECURRING_OPTIONS = ["True", "False"]


def validate_transaction(
    txn_type,
    amount,
    category,
    subcategory,
    merchant,
    payment_mode,
    is_recurring,
    record_status,
):
    txn_type = txn_type.strip().lower()
    category = category.strip().lower()
    subcategory = subcategory.strip().lower()
    merchant = merchant.strip()
    payment_mode = payment_mode.strip().lower()
    is_recurring = is_recurring.strip()
    record_status = record_status.strip()

    if txn_type not in TXN_TYPE_ALLOWED_CATEGORIES:
        raise ValueError("Invalid transaction type.")

    if category not in TXN_TYPE_ALLOWED_CATEGORIES[txn_type]:
        raise ValueError(f"Category '{category}' is not allowed for transaction type '{txn_type}'.")

    if category not in CATEGORY_DATA:
        raise ValueError("Invalid category.")

    if subcategory not in CATEGORY_DATA[category]:
        raise ValueError(f"Subcategory '{subcategory}' is not valid for category '{category}'.")

    if merchant not in CATEGORY_DATA[category][subcategory]:
        raise ValueError(f"Merchant '{merchant}' is not valid for selected category and subcategory.")

    if payment_mode not in PAYMENT_MODES:
        raise ValueError("Invalid payment mode.")

    if record_status not in RECORD_STATUS_OPTIONS:
        raise ValueError("Invalid record status.")

    if is_recurring not in RECURRING_OPTIONS:
        raise ValueError("Invalid recurring value.")

    try:
        amount_decimal = Decimal(amount)
    except InvalidOperation:
        raise ValueError("Amount must be numeric.")

    if amount_decimal <= 0:
        raise ValueError("Amount must be greater than 0.")

    return {
        "txn_type": txn_type,
        "amount_decimal": amount_decimal,
        "category": category,
        "subcategory": subcategory,
        "merchant": merchant,
        "payment_mode": payment_mode,
        "is_recurring": is_recurring,
        "record_status": record_status,
    }


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        conn = None
        cursor = None

        try:
            # IMPORTANT: these keys must match your HTML form field names
            txn_type = request.form.get("txn_type", "").strip()
            amount = request.form.get("amount", "").strip()
            category = request.form.get("category", "").strip()
            subcategory = request.form.get("subcategory", "").strip()
            merchant = request.form.get("merchant", "").strip()
            payment_mode = request.form.get("payment_mode", "").strip()
            is_recurring = request.form.get("is_recurring", "").strip()
            record_status = request.form.get("record_status", "").strip()

            cleaned = validate_transaction(
                txn_type=txn_type,
                amount=amount,
                category=category,
                subcategory=subcategory,
                merchant=merchant,
                payment_mode=payment_mode,
                is_recurring=is_recurring,
                record_status=record_status,
            )

            is_recurring_bool = cleaned["is_recurring"] == "True"
            today = date.today()

            insert_query = """
                INSERT INTO PERSONAL_FINANCE_ANALYTICS_PIPELINE.BRONZE.TRANSACTIONS_RAW (
                    TXN_DATE,
                    UPDATED_AT,
                    TXN_TYPE,
                    AMOUNT,
                    CATEGORY,
                    SUBCATEGORY,
                    MERCHANT,
                    PAYMENT_MODE,
                    IS_RECURRING,
                    RECORD_STATUS
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            conn = get_connection()
            cursor = conn.cursor()

            cursor.execute(
                insert_query,
                (
                    today,
                    today,
                    cleaned["txn_type"],
                    cleaned["amount_decimal"],
                    cleaned["category"],
                    cleaned["subcategory"],
                    cleaned["merchant"],
                    cleaned["payment_mode"],
                    is_recurring_bool,
                    cleaned["record_status"],
                ),
            )

            conn.commit()
            flash("Transaction inserted successfully.", "success")
            return redirect(url_for("index"))

        except Exception as e:
            flash(f"Error: {str(e)}", "danger")
            return redirect(url_for("index"))

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    return render_template(
        "index.html",
        category_data=CATEGORY_DATA,
        txn_type_allowed_categories=TXN_TYPE_ALLOWED_CATEGORIES,
        payment_modes=PAYMENT_MODES,
        record_status_options=RECORD_STATUS_OPTIONS,
        recurring_options=RECURRING_OPTIONS,
    )


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)