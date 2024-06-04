
# Load the snps datasets to CannSeek  postgres 
#
# To Do before running:
# 1. save  input files in host directory mapped to container /transfer
# 2. ln -s the files in 1. to the location of this (load_snp.py) script 
# 3. modify dataset, the samplesfile, posfile, snpefffile, snpeffannotfile, organism_id 
# 4. modify connection strings connstr if needed
# 5. for reruns due to unfinished prevous loading, set max_snpfeatureid_prev to previous maximum value 
# 6  set DROP_TMP_TABLES=True to delete generated temporary loading tables
# 7. run python load_snp.py
#


# MODIFY HERE
#trichomes26_rnaseq_cs10
if False:
	samplesfile='cs10-26trichomes-all-allsnps.samples.txt'
	posfile='cs10-26trichomes-all-allsnps.snppos.txt'
	dataset='26trich_cs10'
	organism_id='3' #cs10
	snpefffile='cs10-26trichomes-all-allsnps.isec_cds.snpeff.vcf.snpeff-table-all.nsyn_syn_spldonacc.txt'
	snpeffannotfile='cs10-26trichomes-all-allsnps.isec_cds.snpeff.vcf.snpeff-table-all.filtered.txt'


# wgs7ds
if False:
	samplesfile='cs10-wgs7ds-genomicsdb-gatk4170-allsnps.samples.txt'
	posfile='cs10-wgs7ds-genomicsdb-gatk4170-allsnps.snppos.txt'
	dataset='wgs7ds_cs10'
	organism_id='3' #cs10
	snpefffile='cs10-wgs7ds-genomicsdb-gatk4170.allsnpsonly-bcftools.snpeff.genic.vcf.gz-highmodlow.txt-synns.txt'
	snpeffannotfile='cs10-wgs7ds-genomicsdb-gatk4170.allsnpsonly-bcftools.snpeff.genic.vcf.gz-highmodlow.txt.filtered.txt'


max_snpfeatureid_prev=-1  # change to previous max_snpfeatureid for reruns due to incomplete loading
connstr=dict()
connstr['user']="postgres"
connstr['password']="postgres"
connstr['host']="10.88.0.21"
connstr['port']="5432"
connstr['database']='public'





# temporary loading tables. manually drop when all done, or set DROP_TMP_TABLES=True
sampletable='tmp_' + dataset + '_sample' 
postable='tmp_' + dataset + '_snppos'
snpefftable='tmp_' + dataset + '_snpeff'
snpeffannottable='tmp_' + dataset + '_snpeffannot'
DROP_TMP_TABLES=False



stock_type_id='42248'  # whole plant
#'1937'

# dataset labels
dataset_sample=dataset + '_samples'
dataset_snppos=dataset 


from sqlalchemy import create_engine

engine = create_engine('postgresql://' + connstr['user'] + ':' + connstr['password'] + '@' + connstr['host'] + ':' + connstr['port'], isolation_level = "AUTOCOMMIT" ) #+ '/' + connstr['database'])

conn = engine.connect()
import pandas as pd


def has_table(tablename):
	cur=conn.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE  table_schema = 'public'  AND    table_name   = '" + tablename + "')")
	print(tablename + ' exists?')
	if cur.fetchone()[0]:
		cur=conn.execute("select count(1) from " + tablename)
		if cur.fetchone()[0]>0:
			print('exists non empty')
			return True 
		else: 
			print('exists empty')
			return False
	else:
		print('not exists')
	return False



import csv
from io import StringIO
import os 

#https://deveshpoojari.medium.com/efficiently-reading-and-writing-large-datasets-with-pandas-and-sql-13e593bd28b4
def copy_insert(table, conn, keys, data_iter):
    # Gets a DBAPI connection that provides a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        string_buffer = StringIO()
        writer = csv.writer(string_buffer)
        writer.writerows(data_iter)
        string_buffer.seek(0)

        columns = ', '.join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=string_buffer)


def create_load_table(fin,tablename,colnames=None,if_exists='fail',method=None):
	#if_exists ‘fail’, ‘replace’, ‘append’}, 

	if method=='copy':
		cmdline='head -n 10 ' + fin + ' > ' + fin + '.head.txt'
		print(cmdline)
		os.system(cmdline)
		fin=fin + '.head.txt'
	if colnames:
		df=pd.read_csv(fin,names=colnames, header=None, sep="\t")
	else:
		df=pd.read_csv(fin)
	fin=fin.replace('.head.txt','')
	print(df)

	if method=='copy':
		print('method=copy')
		conn.execute("drop table if exists " + tablename)
		print('dropped ' + tablename)
		df.to_sql(tablename, engine,if_exists=if_exists,index=False)
		print('method = copy, truncate table and reload ' + fin)
		cur=conn.execute("truncate table  " + tablename)
		cmdline='copy ' + tablename + " FROM '/transfer/" + fin + "' WITH CSV DELIMITER E'\t'"
		print(cmdline)
		cur=conn.execute(cmdline) #.execution_options(autocommit=True)
		
	elif method:
		print('method=' + method)
		df.to_sql(tablename, engine,if_exists=if_exists, index=False, method=method)
	else:
		print('method=None')
		df.to_sql(tablename, engine,if_exists=if_exists,index=False)

	cur=conn.execute("select count(1) from " + tablename)
	print(tablename + ' loaded rows')
	print(cur.fetchone())
	cur = conn.execute("select * from " + tablename + " limit 2")
	print(cur.fetchall())


def get_sequence_max(table):

	cur=conn.execute("select count(1) from " + table)
	if cur.fetchone()[0]>0:
		cur=conn.execute("select max("+  table + "_id) from " +  table)
		print("max(" + table + "_id)" )
		#print(cur.fetchone())
		return cur.fetchone()[0]
	else:
		print("max(" + table + "_id)=0")
		return 0


def count_rows(tablename):
	cur=conn.execute("select count(1) from " +  tablename )
	return cur.fetchone()[0]

def columnval_exists(tablename, column, value):
	cur=conn.execute("select count(1) from " +  tablename + " where " + column + "='" + value  + "'")
	return cur.fetchone()[0]>0


def columnval_exists_any(tablename, column, fromtable, fromcolumn):
	cur=conn.execute("select count(1) from " +  tablename + " t1, " + fromtable + " t2 where  t1. " + column + "=t2." + fromcolumn)
	return cur.fetchone()[0]>0




if True:  # load temporary tables

	print(has_table(sampletable))
	if not has_table(sampletable):
		create_load_table(samplesfile,sampletable,colnames=['id','name'],if_exists='replace')

	print(has_table(postable))
	if not has_table(postable):
		create_load_table(posfile,postable,colnames=['id','contig','position','ref','alt'],if_exists='replace',method='copy')

	print(has_table(snpefftable))
	if not has_table(snpefftable):
		create_load_table(snpefffile,snpefftable,colnames=['contig','position','variant_type','alt'],if_exists='replace',method='copy')

	if True:
		conn.execute("CREATE INDEX IF NOT EXISTS " + sampletable + "_name_idx ON " + sampletable + "(name)")
		conn.execute("CREATE INDEX IF NOT EXISTS " + postable + "_contig_idx ON " + postable + "(contig)")
		#conn.execute("ALTER TABLE " + postable + " ADD COLUMN IF NOT EXISTS pos0 int") 
		#conn.execute("CREATE INDEX IF NOT EXISTS " + postable + "_pos0_idx ON " + postable + "(pos0)")
		conn.execute("CREATE INDEX IF NOT EXISTS " + snpefftable + "_variant_type_idx ON " + snpefftable + "(variant_type)")
		print('index created for tmp tables')


#  dataset ids (sampleset, snpset)

if not columnval_exists('db','name',dataset_sample):
	cur=conn.execute("insert into db(name,description) values('" + dataset_sample + "','" + dataset_sample + "')")
	print('inserted db ' +  dataset_sample)
	print(cur.rowcount)

cur=conn.execute("select db_id from db where name='" + dataset_sample  + "'")
db_id=cur.fetchone()[0]
print("db_id=" + str(db_id))


if not columnval_exists('variantset','name',dataset_snppos):
	cur=conn.execute("insert into variantset(name,description,variant_type_id,organism_id) values('" + dataset_snppos + "','" + dataset_snppos + "',855," + organism_id + ")")
	print('inserted variantset ' +  dataset_snppos)
	print(cur.rowcount)

cur=conn.execute("select variantset_id from variantset where name='" + dataset_snppos  + "'")
vs_id=cur.fetchone()[0]
print("variantset_id=" + str(vs_id))
print('inserted temp tables aand sample and variant sets')


if True: # samples tables

	if True: #not columnval_exists_any("dbxref","accession",sampletable,"name"):
		cur=conn.execute("insert into dbxref(db_id,accession) select " +  str(db_id) + " as db_id, " + " name from " + sampletable + " on conflict(db_id,accession,version) do nothing")
		print('inserted dbxref ' +  sampletable)
		print(cur.rowcount)
	else:
		print('exists in dbxref ' +  sampletable)

	if True: #not columnval_exists_any("stock","uniquename",sampletable,"name"):
		cur=conn.execute("insert into stock(dbxref_id,organism_id,type_id,name,uniquename) select dx.dbxref_id," + organism_id + " as organism_id, " + stock_type_id + " as type_id, dx.accession as name, dx.accession as uniquename from dbxref dx where dx.db_id=" +  str(db_id) + " on conflict (organism_id, uniquename, type_id) do nothing")
		print('inserted stock ' +  sampletable)
		print(cur.rowcount)
	else:
		print('exists in stock ' +  sampletable)


	if True: # don't rerun if previously successful
		sql="insert into stock_sample(stock_id,dbxref_id) select s.stock_id, dx.dbxref_id from dbxref dx, stock s where s.dbxref_id=dx.dbxref_id and dx.db_id=" +  str(db_id) + " on conflict(stock_id,dbxref_id) do nothing"
		print(sql)
		cur=conn.execute(sql)
		print('inserted stock_sample ' +  str(cur.rowcount))

	if True: # don't rerun if previously successful
		sql="insert into sample_varietyset(stock_sample_id,db_id,hdf5_index) select ss.stock_sample_id," + str(db_id) + " as db_id, t.id-1 as hdf5_index from " +  sampletable + " t, stock_sample ss, dbxref dx where t.name=dx.accession and dx.dbxref_id=ss.dbxref_id  on conflict(stock_sample_id,db_id,hdf5_index) do nothing"
		print(sql)
		cur=conn.execute(sql)
		print('inserted sample_varietyset ' +  str(cur.rowcount))

	print('samples related tables inserted')




if True:  # snppos tables

	max_snpfeatureid=get_sequence_max('snp_feature')
	print('actual max_snpfeatureid=' + str(max_snpfeatureid))
	if max_snpfeatureid_prev>-1:
		max_snpfeatureid=max_snpfeatureid_prev
		print('max_snpfeatureid is set to previous attemp value')
	print('max_snpfeatureid=' + str(max_snpfeatureid))
	print('keep this value. In case loading is not successful, set max_snpfeatureid to this value in next attempt')
	#max_snpfeatureid=previous_max_snpfeatureid  reset to previous value if previous snp_featureloc insert was not successful but snp_feature was 
	count_postable=count_rows(postable)

	if True: 
		sql="SELECT s+" + str(max_snpfeatureid) + " as snp_feature_id FROM generate_series(1, " + str(count_postable) + ") s"; 
		sql="insert into snp_feature(snp_feature_id) " + sql + " on conflict(snp_feature_id) do nothing"
		print(sql)
		cur=conn.execute(sql)
		print('inserted snp_feature ' +  str(cur.rowcount))

	if True:
		sql="insert into snp_featureloc(snp_feature_id,organism_id,position,refcall,srcfeature_id)  select  t.id+" + str(max_snpfeatureid) + " as snp_feature_id, " + organism_id + " as organism_id, t.position-1 as  position, t.ref as refcall, f.feature_id as srcfeature_id  from  " + postable + " t, feature f where  f.name=t.contig on conflict(snp_feature_id,srcfeature_id,position) do nothing" 
		print(sql)
		cur=conn.execute(sql)
		print('inserted snp_featureloc ' +  str(cur.rowcount))

	if True:
		sql="insert into variant_variantset(variant_feature_id,variantset_id,hdf5_index) select  t.id+" + str(max_snpfeatureid) + " as variant_feature_id, " + str(vs_id) + " as variantset_id, t.id-1 as hdf5_index from " + postable + " t  on conflict (variant_feature_id,variantset_id) do nothing"
		print(sql)
		cur=conn.execute(sql)
		print('inserted variant_variantset ' +  str(cur.rowcount))

	if True:
		sql="insert into  platform(variantset_id, db_id ) values( " + str(vs_id) + "," + str(db_id) + ") on conflict(variantset_id, db_id) do nothing" 
		print(sql)
		cur=conn.execute(sql)
		print('inserted platform ' +  str(cur.rowcount))
		#[platformid] = cur.fetchone()


	cur=conn.execute('select platform_id from platform where variantset_id=' + str(vs_id) + ' and db_id=' + str(db_id))
	[platformid] = cur.fetchone()
	print('platformid=' + str(platformid))
	sql="insert into genotype_run(platform_id,data_location) values(" + str(platformid) + ",'"  + dataset  +".h5') on conflict(platform_id,data_location) do nothing" 
	print(sql)
	cur=conn.execute(sql)
	print('inserted genotype_run ' +  str(cur.rowcount))

	print('inserted snppos, platforms')


# snpeff props  (synonymous/nonsynonymous snps)
if True:
	sql="ALTER TABLE " + snpefftable + " ADD COLUMN IF NOT EXISTS pos0 int"
	print(sql)
	cur=conn.execute(sql)
	sql="update " + snpefftable + " set pos0=position-1"
	print(sql)
	cur=conn.execute(sql)
	sql="CREATE INDEX IF NOT EXISTS " + snpefftable + "_contig_pos0_idx ON " + snpefftable + "(contig, pos0)"
	print(sql)
	cur=conn.execute(sql)
	sql="CREATE INDEX IF NOT EXISTS " + snpefftable + "_variant_type_idx ON " + snpefftable + "(variant_type)"
	print(sql)
	cur=conn.execute(sql)


	sql="insert into snp_featureprop(snp_feature_id,type_id,value) select distinct sfl.snp_feature_id, ct.cvterm_id as type_id, t.alt as value from " + snpefftable + " t, cvterm ct , snp_featureloc sfl, feature srcf where ct.name=t.variant_type and srcf.feature_id=sfl.srcfeature_id and srcf.name=t.contig and sfl.position=t.pos0 order by sfl.snp_feature_id on conflict(snp_feature_id, type_id, value) do nothing"
	print(sql)
	cur=conn.execute(sql)
	print('inserted snp_featureprop ' +  str(cur.rowcount))


# snpeff annots (snpseff details page)
if True: 

	if not has_table(snpeffannottable):
		create_load_table(snpeffannotfile,snpeffannottable,colnames=['contig','position','ann','lof','nmd','position_minus1'],if_exists='append')

	cur=conn.execute("select count(1) from " + snpeffannottable)
	print('new snpeffs=' + str(cur.fetchone()[0]))
	sql="insert into tmp_snpeff select  * from " + snpeffannottable + " on conflict(contig,position) do nothing"
	print(sql)
	cur=conn.execute(sql)
	print('inserted tmp_snpeff ' +  str(cur.rowcount))


if DROP_TMP_TABLES:
	cur=conn.execute("drop table " + sampletable + ", " + postable + ", " + snpefftable + ", " + snpeffannottable) 


cur.close()
conn.close()

