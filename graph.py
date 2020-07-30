import pickle
import matplotlib.pyplot as plt
import statistics

fp = "bigquery-public-data.gnomAD.v2_1_1_exomes__chr1_mutations"

with open(fp, "rb") as file:
    arr = pickle.load(file)

minimum  = min(arr)
maximum = max(arr)
average = sum(arr) / len(arr)
median = statistics.median(arr)

print(minimum, maximum, average, median)

x_pos = [i for i in range(len(arr))]

plt.figure(dpi=1200)
plt.bar(x_pos, arr)
plt.xlabel("Start Position (100k) ")
plt.ylabel("# of Mutations with Frequency <= 0.01")
plt.title("Rare mutations count from Chr1 Exomes with 100k bin size")

plt.savefig("exomes__chr1_mutations.pdf")
plt.show()
