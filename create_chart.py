import matplotlib.pyplot as plt
import query

def create_chart():
    df = query.load("openfoodfacts_prices")
    
    plt.figure(figsize=(10, 6))
    
    # Create a combined label for x-axis
    df['label'] = df['product_name'] + "\n(" + df['retailer'] + ")"
    
    plt.bar(df['label'], df['price'], color=['#7fcdbb', '#41b6c4', '#1d91c0', '#225ea8'])
    plt.ylabel('Price (USD)')
    plt.title('Avocado and Egg Prices (OpenFoodFacts)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    output_path = "openfoodfacts_prices_chart.png"
    plt.savefig(output_path)
    print(f"Chart saved to {output_path}")

if __name__ == "__main__":
    create_chart()
