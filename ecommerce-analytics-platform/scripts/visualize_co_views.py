import pandas as pd
import matplotlib.pyplot as plt

# Read Parquet output
parquet_path = "E:\\BigData FinalProject\\generated_data\\clv.parquet"
df = pd.read_parquet(parquet_path)

# Prepare data for visualization
top_customers = df.nlargest(10, 'total_spent')[['user_id', 'total_spent']]

# Plot bar chart
plt.figure(figsize=(10, 6))
plt.bar(top_customers['user_id'], top_customers['total_spent'], color='lightgreen')
plt.xlabel('User ID')
plt.ylabel('Total Spent ($)')
plt.title('Top 10 Customers by Total Spending')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('E:\\BigData FinalProject\\clv_customers.png')
plt.show()