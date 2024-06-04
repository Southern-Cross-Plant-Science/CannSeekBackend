'''
Process dump of SNPEff results 
The input is the output from 
/path/to/bcftools query  -f "%CHROM\t%POS\t%INFO/ANN\t%INFO/LOF\t%INFO/NMD\t%POS0\n"  ${VCFNAME}.snpeff.vcf.gz | grep -P "\|HIGH\||\|MODERATE\|\|LOW\|" > ${VCFNAME}.snpeff.highmidlow.txt

Separate SNP effects annotations by variant type, generating separate snp list for each variant type
Concatenate the types of interest (like missense_variant and synonymous_variant)

This also generates *.filtered.txt file, which list the detailed annotation

The generated files are loaded to the CannSeek database using load_snp.py

'''
import sys
import os

VCFNAME='/path/to/snpeffvcffile' 
BCF='/path/to/bcftools'
os.system(BCF + ' query  -f "%CHROM\t%POS\t%REF\t%INFO/ANN\t%INFO/LOF\t%INFO/NMD\t%POS0\n"  ' + VCFNAME + ' | grep -P "\|HIGH\||\|MODERATE\|\|LOW\|" > ' + VCFNAME + '.highmidlow.txt')

inf=VCFNAME + '.highmidlow.txt'
#inf=sys.argv[1]
variants2pos=dict()
with open(inf) as  fin, open(inf+'.filtered.txt','wt') as fout:
	line=fin.readline()
	while line:
		cols=line.rstrip().split()
		anns=cols[2].split(",")
		filanns=[]
		for ann in anns:
			annatts=ann.split("|",4)
			if annatts[2] in ['MODERATE','HIGH','LOW']:
				filanns.append(ann)
				#print(annatts[1].split("&"))
				for annattsi in annatts[1].split("&"):
					varpos=[]
					if annattsi in  variants2pos:
						varpos=variants2pos[annattsi]
					else:
						variants2pos[annattsi]=varpos
					varpos.append([cols[0],cols[1],annattsi,annatts[0]])

		cols[2]=",".join(filanns)
		fout.write("\t".join(cols) + "\n")
		line=fin.readline()

print('variants=' + str(list(variants2pos.keys())))
for variants in list(variants2pos.keys()):
	print('created ' + inf + '.' + variants + '.txt')
	with open(inf + '.' + variants + '.txt',"wt") as fout:
		for p in variants2pos[variants]:
			fout.write("\t".join(p) + "\n")
			

print('concatenate synonymous and missence_variant list')
os.system('cat ' + inf + '.missense_variant.txt ' + inf + '.synonymous_variant.txt > ' + inf + '.misssyn.txt')

