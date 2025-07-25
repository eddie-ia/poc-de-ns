az group create --name rg-covid-dev-eus --location eastus

az storage account create \
  --name stcoviddevlake01 \
  --resource-group rg-covid-dev-eus \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hns true

az storage account create \
  --name stcovidbackup01 \
  --resource-group rg-covid-dev-eus \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hns true

az datafactory create \
  --resource-group rg-covid-dev-eus \
  --factory-name df-covid-dev-eus \
  --location eastus

az databricks workspace create \
  --resource-group rg-covid-dev-eus \
  --name dbw-covid-dev-eus \
  --location eastus \
  --sku standard
