import pandas as pd
from scipy.stats import wilcoxon

# FILE = "results2.csv"
# COL_A = "baseline"
# COL_B = "tlags_tlag1_dr_mod_gs__pred_growseasdummy"

# df = pd.read_csv(FILE)[[COL_A, COL_B]].dropna()
# a, b = df[COL_A], df[COL_B]

# W2, p_value = wilcoxon(a, b, alternative="two-sided",  zero_method="wilcox")

# d = a - b  # lower RMSE is better. positive means B lower
# print("pairs", len(df))
# print("median(A-B)", d.median())
# print("mean(A-B)", d.mean())
# print("p value", p_value)



FILE = "results2.csv"
FIXED = "tlags_tlag1_dr_mod_gs__pred_growseasdummy"
OUT = "wilcoxon_best_perform.csv"

df = pd.read_csv(FILE)
cols = [c for c in df.columns if c != FIXED]

rows = []
for c in cols:
    pair = df[[FIXED, c]].dropna()
    a = pair[FIXED]
    b = pair[c]
    d = (a - b).round(6)  # pass differences per SciPy guidance

    p_value = wilcoxon(d, alternative="two-sided", zero_method="wilcox", method="auto").pvalue
    
    rows.append({
        "feature": c,
        "pairs": len(pair),
        "median(A-B)": d.median(),
        "mean(A-B)": d.mean(),
        "p_value": p_value,
    
    })

pd.DataFrame(rows).to_csv(OUT, index=False)
print(f"saved {OUT}")