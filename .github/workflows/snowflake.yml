import snowflake.connector
import os

print("Connecting to Snowflake...")

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    role=os.environ["SNOWFLAKE_ROLE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"]
)

cursor = conn.cursor()

print("Connected successfully.")

try:
    cursor.execute("""
        COPY INTO my_table
        FROM @my_stage
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = 'CONTINUE';
    """)
    print("Data loaded successfully using COPY INTO.")

except Exception as e:
    print("Error during COPY INTO:", e)

finally:
    cursor.close()
    conn.close()
    print("Connection closed.")
