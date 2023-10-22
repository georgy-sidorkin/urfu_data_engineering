path_file = "tasks/text_1_var_80"

with open(path_file) as f:
    lines = f.readlines()

words_freq = {}

for l in lines:
    for mark in ".,!?":
        l = (l.replace(mark, " ")
             .strip())

    words = l.split()

    for word in words:
        words_freq[word] = words_freq.get(word, 0) + 1

words_freq = dict(sorted(words_freq.items(), reverse=True, key=lambda x: x[1]))

with open("results/text_1.txt", "w") as res:
    for k, v in words_freq.items():
        res.write(f"{k}: {v}\n")