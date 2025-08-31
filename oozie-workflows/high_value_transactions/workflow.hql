INSERT OVERWRITE DIRECTORY '/user/hadoop/output/high_value_transactions'
SELECT * FROM sales_db.transactions WHERE amount > 150;
