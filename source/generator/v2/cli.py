import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="E-commerce Synthetic Data Generator V2")
    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--products", type=int, default=300)
    parser.add_argument("--orders", type=int, default=5000)
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--stage", choices=["master", "orders", "payments"], required=True)
    parser.add_argument("--insert", action="store_true", help="Persist generated records to ecommerce_test")
    args = parser.parse_args()
    if min(args.customers, args.products, args.orders, args.months) <= 0:
        parser.error("--customers, --products, --orders and --months must be positive")
    return args

