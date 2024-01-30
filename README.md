# SDG661-Fresh-Water
This project aims to analyze the temporal changes of pemanenet water and resovoirs at various basin levels.

## Delta Methods: Relative Changes 
Evaluate the relative changes between the report period (2017-2021) and baseline (2000-2019) 

$\delta = \frac{\gamma-\beta}{\beta + \epsilon}$ * 100 \
where $\delta$ denotes the percentage of change in spatial extent, $\beta$ denotes the median spatial extent for the baseline period (2000-2019), while $\gamma$ denotes the median spatial extent for the report period (2017-2019). 
To handle the case when $\beta = 0$, we used $\epsilon = 1e^{-15}$ by default.

Low and high thresholds based on $\mu$ and $\sigma$: \
$\delta_{low.thd} = \mu - \alpha * \sigma$ \
$\delta_{high.thd} = \mu + \alpha * \sigma$ \
where $\mu$ and $\sigma$ denote the mean and standard deviation respectively, and $\alpha \in [0.5, 1, 1.5, 2, 2.5, 3, 4, 5]$.

## U-Test (or T-Test) Methods
Evaluate the significance of difference bewteen two groups, i.e., report period (2017-2021) and baseline (2000-2019)

Null Hypothesis: The null hypothesis (H0) is a general statement or default position that there is no relationship between two measured phenomena or no association among groups. 

p-value helps determine whether the observed differences or effects are statistically significant, that is, unlikely to have occurred by chance.

- A low p-value indicates that, assuming the null hypothesis is true, there is a low probability of obtaining a result as extreme as the one observed. This is usually taken as evidence that the null hypothesis is false. 

- A high p-value suggests that your observed results can occur as a result of random chance, and there is insufficient evidence to conclude that the effect exists.

### T-Test
The t-score in a t-test is a statistic that indicates how far a sample mean deviates from the population mean in units of standard error. The sign of the t-score (positive or negative) provides information about the direction of this deviation. The 

### U-Test
The U-score in the Mann-Whitney U-test, serves a similar purpose to the t-score in the t-test, but it is used in a different context. The Mann-Whitney U test is a non-parametric test used to compare two independent samples, particularly when the assumptions necessary for the t-test (like normally distributed data) are not met. The U-score represents the number of times observations in one sample precede observations in the other sample when all observations are ranked together.

To understand which group has higher or lower ranks (which might be interpreted as better or worse performance, higher or lower measurements, etc.), you need to look at the descriptive statistics (like median or mean ranks) of the two groups.

```python 
import scipy.stats as stats
# T-test (https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.ttest_ind.html)
# If equal_var is True (default), perform a standard independent 2 sample test that assumes equal population variances. If False, perform Welch’s t-test, which does not assume equal population variance .
t_score, p_t = stats.ttest_ind(data1, data2, equal_var=False)

# U-Test
u_score, p_u = stats.mannwhitneyu(data1, data2)
median2 = np.median(data2)
median1 = np.median(data1)
median_diff = median2 - median1
u_score = median_diff / np.abs(median_diff) * u_score
```

## codes
```python 
# convert gaul_0 input data into same format as the other basin levels (3-8).
python convert_gaul_0_data.py 

python sdg661_delta.py # generate delta csv
python sdg661_utest.py # generate ttest (utest) csv

sdg661.ipynb # test and results analysis
```

## Results
outputs_delta/ \
├── Pemanent_water_delta_2017_allThd.csv <sup><sub>(thresholding 2017-2021 delta with the mean and std from baseline periods 2000-2019)</sup></sub>\
├── Pemanent_water_delta_2017_thd.csv <sup><sub>(thresholding 2017-2021 detla with the mean and std from 2017-2021 report period)</sup></sub> \
├── Pemanent_water_delta.csv <sup><sub>(thresholding all periods' delta with the mean and std from baseline periods 2000-2019)</sup></sub> \
├── Reservoirs_delta_2017_allThd.csv \
├── Reservoirs_delta_2017_thd.csv \
├── Reservoirs_delta.csv 

outputs_ttest/ \
├── Pemanent_water_ttest_2017.csv (u-test and t-test results)\
├── Reservoirs_ttest_2017.csv (u-test and t-test results)



