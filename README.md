# BLAG: Improving the Accuracy of Blacklists
This repository consists of necessary scripts to run BLAG. 

![BLAG pipeline](figs/blag_pipeline_8.png)

## Requirements
`pip install surprise pandas`

[Surprise](http://surpriselib.com/) is a Python package for running recommendation systems. 

## BLAG Configuration
The config file stores the value for different parameters used in BLAG. Please refer our [paper](https://steel.isi.edu/members/sivaram/papers/BLAG_NDSS.pdf) for more explanation.


`known_legitimate_senders=known_legitimate_senders`

Known legitimate senders for a network. Used to curate the misclassification blacklist.

`processed_ips_files=blacklist_ips`

Processed blacklist data. Source of Blacklist data

`l=30`

Constant L for exponential decay. L ensures recently listed blacklisted addresses are allocated a high relevance score compared to addresses that were listed way back in the past.

`K=5`

Constant K for recommendation system. K determines the number of latent features that contribute to an address to be listed in a blacklist

`results_folder=blag_ips`

Write BLAG results folder

`avoid_blacklists=`

Avoid blacklists
