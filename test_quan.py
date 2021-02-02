
##import sys 
##for line in sys.stdin:
##    a = line.split()
str1 = 'abc'
len_str = len(str1)
for i in range(len_str):
    for j in range(len_str):
        if str1[i] == str1[j]:
            continue
        for o in range(len_str):
            if str1[o] == str1[j] or str1[o] == str1[i]:
                continue
            new_str = str1[i] + str1[j] + str1[o]
            print(new_str)
