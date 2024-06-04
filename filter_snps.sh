
# separate SNPs from indels
BCF=/path/to/bcftools
THREADS=8
VCFNAME=/path/to/vcffile	# vcf filename (exclude .vcf.gz)
REF=/path/to/reference.fasta
$BCF norm --atom-overlaps . -c w -a --fasta-ref $REF  --threads $THREADS  ${VCFNAME}.vcf.gz | $BCF filter --threads $THREADS -i 'TYPE="snp"' | $BCF norm --threads $THREADS  -m +any -Oz -o ${VCFNAME}.allsnpsonly-bcftools.vcf.gz
