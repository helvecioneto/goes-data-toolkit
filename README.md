# goes-data-toolkig
GOES Data Toolkit

## Download package
git clone https://github.com/helvecioneto/goes-data-toolkit/

## Install dependencies
```bash
cd goes-data-toolkit
conda env create -n goes-ata-toolkit --file goes-data-toolkit.yml
conda activate goes-ata-toolkit
```

## Exemple of usage:
Download data: start_date = '2020-06-06 08:00', end_date = '2020-06-06 22:00', between times = '09:00,22:00', time_interval 10min, Satellite = goes16, channel = 02 and Provider =  AWS

```python
cd goes-data-toolkit
python goes-toolkit -s '2020-06-06 08:00' -e '2020-06-06 22:00' -bt '09:00,22:00' -i 10min -sat goes16 -c 02 -p AWS download
```
