# 
# The VCF should contain SNPs only (to filter, use filter_snps.sh)
# Process SNP vcf to generate files for loading to CannSeek
# generates HDF5, snp position list, and samples list 
#
# The generated files snppos.txt, samples.txt are loaded to the database using load_snp.py
# The generated ${DATASET}_tran.h5 is placed in the tomcat flatfile directory, registered as ${DATASET}.h5 in genotype_run.data_location table
#

VCFNAME=/path/to/vcffile #(exclude vcf.gz)
BCF=/path/to/bcftools
LOADMATRIX_GENO=/path/to/loadmatrix_geno


$BCF query -f '%CHROM\t%POS\t%REF\t%ALT[\t%TGT]\n' ${VCFNAME}.vcf.gz | tr -d "/" | sed 's/\*/./g;s/\.[|]\./../g;s/|//g;s/\t.\t/\t..\t/g;s/\t.\n/\t..\n/g' > ${VCFNAME}-mat_vcf-clean.txt 


$BCF query -f '%CHROM\t%POS\t%REF\t%ALT\n' ${VCFNAME}.vcf.gz | awk '{print NR,$0}' | sed 's/ /\t/' >  ${VCFNAME}.snppos.txt
$BCF query -l ${VCFNAME}.vcf.gz | awk '{print NR,$0}' | sed 's/ /\t/' >  ${VCFNAME}.samples.txt

rows=`wc -l "${VCFNAME}".snppos.txt`
samples=`"${BCF}" query -l  "${VCFNAME}".vcf.gz | wc -l`

echo "${rows} rows, ${samples} samples\n"

$LOADMATRIX_GENO  -m 5000  -n $samples  -r $rows -t -o ${VCFNAME}_tran.h5 -i ${VCFNAME}-mat_vcf-clean.txt

