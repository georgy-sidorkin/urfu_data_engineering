import pandas as pd

path_file = "tasks/text_4_var_80"

# read csv file
df = pd.read_csv(path_file, header=None)

# rename columns
df = df.rename(columns={0: "id",
                        1: "name",
                        2: "surname",
                        3: "age",
                        4: "salary",
                        5: "phone"
                        })

# drop column phone
df = df.drop("phone", axis=1)

# remove rouble symbol
df.salary = (df.salary
             .transform(lambda x: int(x.replace("₽", ""))))

# values for filters
mean_salary = df.salary.mean()
filter_age = 25 + 80 % 10

# filter and sort dataframe
df = (df
      .query("salary >= @mean_salary and age > @filter_age")
      .sort_values(by="id"))

df.to_csv("results/text_4.csv", sep=",", index=False)
