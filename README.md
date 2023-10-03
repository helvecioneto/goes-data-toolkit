# goes-data-toolkig
GOES Data Toolkit

## Download package
git clone https://github.com/helvecioneto/goes-data-toolkit/

## Install dependencies
cd goes-data-toolkit
conda env create -n goes-ata-toolkit --file goes-data-toolkit.yml
conda activate goes-ata-toolkit

## Exemple of usage:
cd goes-data-toolkit
python goes-toolkit -s '2020-06-06 08:00' -e '2020-06-06 22:00' -bt '09:00,22:00' -i 10min -sat goes16 -c 02 -p AWS download
