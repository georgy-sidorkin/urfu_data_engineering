path_file = "tasks/text_3_var_80"

with open(path_file) as f:
    lines = f.readlines()

numbers = []

for line in lines:
    nums = line.strip().split(",")
    for i in range(len(nums)):
        if nums[i] == 'NA':
            nums[i] = str((int(nums[i - 1]) + int(nums[i + 1])) / 2)
    numbers.append(list(filter(lambda x: float(x) ** 0.5 >= 50 + 80, nums)))

with open("results/text_3.txt", "w") as res:
    for i in numbers:
        res.write(f"{','.join(i)}\n")
