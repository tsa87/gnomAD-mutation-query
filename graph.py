import matplotlib.pyplot as plt
import pandas as pd

fp = "bigquery-public-data.gnomAD.v2_1_1_exomes__chr1_mutations"

def plot_from_csv(
    csv_path, 
    x_index,
    y_index,
    x_label,
    y_label,
    plot_title,
    save_path=None):
        
    
    x_values = []
    y_values = []
    
    with open(csv_path, "r+") as f:
        next(f) #skip heading
        for line in f:
            tokens = line.split(",")
            x_values.append(int(float(tokens[x_index])))
            y_values.append(int(float(tokens[y_index])))
            
    plt.figure(dpi=1200)
    plt.bar(x_values, y_values)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    
    if save_path is None:
        plt.savefig(plot_title)
    else:
        plt.savefig(save_path)
        

if __name__ == "__main__": 
    paths = [
        "bigquery-public-data.gnomAD.v3_genomes__chr17.csv",
        "bigquery-public-data.gnomAD.v3_genomes__chr1.csv",
        "bigquery-public-data.gnomAD.v3_genomes__chr20.csv",
        "bigquery-public-data.gnomAD.v3_genomes__chrX.csv",  
        "bigquery-public-data.gnomAD.v3_genomes__chrY.csv"
    ]
    
    for path in paths:
        plot_from_csv(path, 1, 3,
            "Base Pair Start Position (100k)",
            "Count of Mutations with Frequency <= 1%",
            "rare_mutations_count_" + ".".join(path.split(".")[:-1]) + ".pdf"
        )
    