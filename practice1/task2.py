path_file = "tasks/text_2_var_80"

with open(path_file) as f:
    lines = f.readlines()

numbers = []

for l in lines:
    numbers.append(list(map(int, l.split())))

with open("results/text_2.txt", "w") as res:
    for i in numbers:
        res.write(f"{sum(i)}\n")
