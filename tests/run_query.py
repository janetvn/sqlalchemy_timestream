from sqlalchemy import create_engine, text

url = 'jdbcapi+timestream://@timestream.ap-southeast-2.amazonaws.com/test?application_name=superset&instance_profile=' \
      'true&role_arn=arn%3Aaws%3Aiam%3A%3A296723344747%3Arole%2Fch-superset'
engine = create_engine(url)
query = text("select * from test.IoTMulti LIMIT 10")
conn = engine.connect()
rows = conn.execute(query)

for row in rows:
    print(row)
