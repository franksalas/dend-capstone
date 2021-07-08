## Enviroments


## Setup enviroment
Create environment from file
```bash
conda env create -f base_environment.yml
```
## Activate environment

```bash
conda activate crdata
```

## Update environment

```bash
$ conda env update --prefix ./env --file environment.yml  --prune

pip freeze > requirements.txt
```


## Export environment

```bash
conda env export > environment.yml
or 
conda env export | grep -v "^prefix: " > environment.yml

```