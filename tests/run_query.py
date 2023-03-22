from sqlalchemy import create_engine, text

url = "jdbcapi+timestream://Region=ap-southeast-2;RoleArn=arn%3Aaws%3Aiam%3A%3A296723344747%3Arole%2Fch-superset;DriverPath=/Users/mainguyen/Documents/amazon-timestream-jdbc-2.0.0-shaded.jar"
engine = create_engine(url)
query = text("select * from test.IoTMulti LIMIT 10")
conn = engine.connect()
rows = conn.execute(query)

for row in rows:
    print(row)
