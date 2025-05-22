# WiClean submission files

## Video

The video is in the file Video.mp4. It is also available
at this Google Drive link:
https://drive.google.com/file/d/1-syawwoiz0CTk-ZrqTj8G9-nuVn9GtDX/view?usp=sharing

## Report

The project report is in the file Report.pdf.
The shasum is 83e8e730070a2a4dccc0d145512d0b1d94838c81.

## Scraper

The data scraper is an aggressively optimized piece of Rust code
that downloads XML files from the Wikipedia database dumps and processes
them into Parquet files. With a reasonable CPU, 
the scraping step is mostly network-bottlenecked
and will take around an hour with a 1gbps connection, and much longer
with a slower connection. Therefore, I don't recommend running it to
completion yourself. Our provided data tarball (see below) already contains
the pre-scraped data.

To run the data scraper, run the `./scraper.sh` script. It will launch
a Docker container to compile and run the scraper. Note that it may require
sudo privileges to create a Docker container depending on your setup.

The scraper will write some temporary files to the `scraper/data` directory.
These can be safely deleted. They are there to allow resuming the scraping
process if it needs to be interrupted.

When running in a Docker container, the scraper doesn't seem to respond
to ctrl-c. You can stop it using docker kill if needed.

At the end of the scraping process, the output files are in `scraper/data/links.parquet`
and `scraper/data/articles.parquet`.

## Jupyter Notebooks

The notebooks contain all our data analysis code, i.e.
the part after the scraping step. They are separated into
three notebooks for preprocessing/feature extraction, pattern
mining, and anomaly detection respectively.

First, download our data tarball from the following Google Drive link into this
directory (the shasum of the tarball is 6936019d8dc290af4fb964553f8fe892f1b5e65a):
[`https://drive.google.com/file/d/1pDECs48g7HU98mjpalhDg2FPewj5mogG/view?usp=sharing`]

Then, run ./extract_data.sh. It will launch a temporary docker container
to decompress the tarball. It may need sudo to create a docker container.
If you can't get it to work, you can try running `tar -xvf data.tar.zst`
if you have the zstd CLI tool installed.

Then, run docker-compose up. This will create a spark master, spark worker,
and Jupyter Notebook container. The notebook will take some time to install
dependencies. Once it's up, you can connect at 127.0.0.1:8888.

The notebooks contain all the Python code used for the data analysis. You can
run the cells (although we have already run them ourselves with the output shown).
Beware that owing to the large size of the dataset,
some of the cells take a lot of memory (~40GiB), so you may want to skip running
them. If you really have to run these and lack the memory, 
you can try CHPC, Colab Pro, or email Caelum (u1220788@utah.edu)
and we can give you access to a machine that can run them.
(We originally ran these notebooks on Colab Pro.)

