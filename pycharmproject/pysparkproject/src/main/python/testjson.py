from common.process.JsonFile import JsonFile
from common.session.Session import Session
import sys
input = sys.argv[1]
if __name__ == "__main__":
    spark = Session().getSparkSession()
    df =  JsonFile().read(spark,input)
    df.printSchema()
    df.select("addresses.city", "addresses.postal_code", "addresses.state", "addresses.street_name",
               "addresses.street_number", "email",
               "first_name", "last_name", "gender", "id", "phone_numbers").registerTempTable("employees")
    spark.sql(
        "select city,postal_code[0] as postal_code_1,postal_code[1] as postal_code_2,postal_code[2] as postal_code_3 ,state,street_name,street_number,email,first_name, last_name,gender, id, phone_numbers[0] as phone_numbers_1, " +
        "phone_numbers[0] as phone_numbers_2 from employees").show()
