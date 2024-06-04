# CannSeekBackend


**What is this repository for?**

Instructions and scripts to start the CannSeek server, and load datasets when needed. 
The Tomcat archive includes the war file and internal directories mapped to the host flatfile directory
The Postgres archive is preloaded with the three references (cs10, pkv5, fnv2) and their gene models, and the cs10 26TRICH SNP dataset.

A separate repository is provided for the CannSeek web application source code and Eclipse project at [CannSeek](https://bitbucket.org/irridev/iric_portal/src/CannSeek)


**Start Postgres database server and Tomcat from the Docker/Podman images and preloaded archive files**

1. Download the archive files (cannseek-26trich.tgz, cannseek_tomcat.tgz, cannseek_flatfiles.tgz) from DOI:GigaDB (Github has file size limit of 100Mb)
2. Extract the archive cannseek-26trich.tgz
3. Extract the archives cannseek_tomcat.tgz and cannseek_flatfiles.tgz  
4. Modify the mapped volumes for postgres  in ``cannseek_start.sh`` to the archive directories in 2  
5. Modify the mapped volumes for tomcat in ``cannseek_start.sh`` to the archive directories in 3  
6. Run ``cannseek_start.sh``  


**VCF file processing and loading**

When a new SNP dataset is available for any of the loaded reference (cs10,pkv5, or fnv2) in VCF format, this section describes the steps to load it into CannSeek.

&nbsp;A. These software are required  
&nbsp;&nbsp;&nbsp;1. bcftools  
&nbsp;&nbsp;&nbsp;2. [SNPEff](https://pcingola.github.io/SnpEff/)    

&nbsp;B. Generate SNP effect annotations using SNPEff on the VCF file using the same reference and available predicted gene models. Follow the [SNPEff manual](https://pcingola.github.io/SnpEff/snpeff/running)

&nbsp;C. For VCF files with both SNPs and indels, filter to include SNPs only    
&nbsp;&nbsp;&nbsp;1. modify REF, VCFNAME and BCF in ``filter_snps.sh``. REF is reference fasta, VCF is vcf file path, BCF is bcftools path  
&nbsp;&nbsp;&nbsp;2. run ``filter_snps.sh``  
	
&nbsp;D. Process and filter (only HIGH,MODERATE, and LOW effects) SNPEff VCF result.  
&nbsp;&nbsp;&nbsp;1. modify VCFNAME and BCF in ``filter-highmodlow.py``. VCFNAME is path of SNP-only vcf with SNPEff annotation. BCF is bcftools path  
&nbsp;&nbsp;&nbsp;2. run ``python filter-highmodlow.py``  
	
&nbsp;E. Generate HDF5 and csv files for loading  
&nbsp;&nbsp;&nbsp;1. modify VCFNAME, BCF and LOADMATRIX\_GENO in ``vcf2h5.sh``. VCFNAME is path of SNP-only vcf, LOADMATRIX_GENO is path to ``loadmatrix_gene`` executable,  BCF is bcftools path  
&nbsp;&nbsp;&nbsp;2. run ``vcf2h5.sh``

	
&nbsp;F. If not yet started, start the CannSeek Postgres database and Tomcat server from the Docker/Podman archives/images as described above.   


&nbsp;G. Load the SNPs data  
&nbsp;&nbsp;&nbsp;1. modify in ``load_snp.py`` the following variables:    
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**dataset**  name of dataset (no space, use only alphanumeric or underscore )  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**samplesfile** samples file  (generated in E.2)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**posfile** positions file (generated in E.2)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**snpefffile** .misssyn.txt snpeffects file (concatenated from missense_variant.txt and synonymous_variant.txt files generated in D.2)   
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**snpeffannotfile** .filtered.txt filtered SNP annotation file (generated in D.2)   
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**organism_id** organism_id in database (3 for cs10, 2 for pkv5, 25 for fnv2 in the provided database)   
&nbsp;&nbsp;&nbsp;2. copy the files defined in G.1 to the host directory mapped to /transfer in the Postgres container    
&nbsp;&nbsp;&nbsp;3. create symbolic links ``ln -s`` to the files in G.1 in the location of ``load_snp.py``    
&nbsp;&nbsp;&nbsp;4. modify the database connection dictionary connstr in ``load_snp.py`` if needed  
&nbsp;&nbsp;&nbsp;5. run ``python load_snp.py``	    
&nbsp;&nbsp;&nbsp;6. rename the .h5 file generated in E.2 to {dataset}_trans.h5 and copy to the TOMCAT host flatfile directory 	
	
