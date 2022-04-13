import os
import copy
import gffutils
import itertools

import numpy as np

from collections import defaultdict
from functools import reduce

def createDatabase (gff_file, remove_pseudogenes = True):

	# Create a GFF database using gffutils or return the gffutils database if it already exists
	gff_db_filename = os.path.join(f"{gff_file}.db")
	if not os.path.isfile(gff_db_filename):
		gff_db = gffutils.create_db(gff_file, gff_db_filename, id_spec = None, merge_strategy = "create_unique")

		# Delete any pseudogenes, if specified
		if remove_pseudogenes:
			pseudogene_gene_features = [feature for feature in gff_db.features_of_type('pseudogene')]
			pseudogenes_children_features = list(itertools.chain.from_iterable(gff_db.children(_pgf) for _pgf in pseudogene_gene_features))
			try:
				pseudogene_tids = [_pgf.attributes['gene'][0] for _pgf in pseudogene_gene_features]
				pseudogenes_tid_features = [feature for feature in gff_db.features_of_type('intron') if feature.attributes['transcript_id'][0] in pseudogene_tids]
			except: pseudogenes_tid_features = []
			gff_db.delete(pseudogene_gene_features + pseudogenes_children_features + pseudogenes_tid_features, make_backup = False)

	# Read the database, if already found
	else: gff_db = gffutils.FeatureDB(gff_db_filename)

	# Return the database
	return gff_db

def removeDatabase (gff_file):
	gff_db_filename = os.path.join(f"{gff_file}.db")
	os.remove(gff_db_filename)

def outputGff (gff_input, gff_output, new_feature_dict, add_tss_elements = False, add_introns = False, add_utrs = False, **kwargs):
	'''
	Output a new GFF by updating the input GFF file.
	'''

	# Assign the ID for the gene/mRNA
	def assignGffID (gff_attributes):
		gff_ID = [gff_entry.split('ID=')[1] for gff_entry in gff_attributes.split(';') if gff_entry.startswith('ID=')]
		if len(gff_ID) == 1: gff_ID = gff_ID[0]
		else: raise Exception(f'Cannot bind ID: {gff_attributes}')
		if ':' in gff_ID: gff_ID = gff_ID.split(':')[0]
		return gff_ID

	# Assign the Parent ID
	def assignGffParent (gff_attributes):
		gff_ID = [gff_entry.split('Parent=')[1] for gff_entry in gff_attributes.split(';') if gff_entry.startswith('Parent=')]
		if len(gff_ID) == 1: gff_ID = gff_ID[0]
		else: raise Exception(f'Cannot bind ID: {gff_attributes}')
		if ':' in gff_ID: gff_ID = gff_ID.split(':')[0]
		return gff_ID

	# Assign the Parent ID for the exon/CDS
	def assignGffParents (gff_attributes):
		for parent in assignGffParent(gff_attributes).split(','): yield parent
	
	# Check if the gene elements are merged
	merged_gff = None
	with open(gff_input, 'r') as gff_data:
		for gff_pos, gff_line in enumerate(gff_data):
			if gff_line.startswith('#'): continue
			gff_entry_list = gff_line.split('\t')
			gff_feature_type = gff_entry_list[2]

			# Assign the current gene
			if gff_feature_type == 'gene':
				gff_gene = assignGffID(gff_entry_list[8])
				children_last_pos = None

			elif gff_feature_type not in ['exon', 'CDS']:
				if 'Parent' not in gff_entry_list[8]: continue
				if assignGffParent(gff_entry_list[8]) == gff_gene:
					if children_last_pos == None:
						children_last_pos = gff_pos
						continue
					if gff_pos - children_last_pos > 1: merged_gff = False
					else: merged_gff = True
					break

	'''
	Scan the exons and CDSs to assign the elements at the
	correct file positions.
	'''
	feature_pos_dict = defaultdict(lambda: defaultdict(int))
	with open(gff_input, 'r') as gff_data:
		for gff_pos, gff_line in enumerate(gff_data):
			if gff_line.startswith('#'): continue
			gff_entry_list = gff_line.split('\t')
			gff_feature_type = gff_entry_list[2]

			# Assign the current gene
			if gff_feature_type == 'gene':
				gff_gene = assignGffID(gff_entry_list[8])
				if add_tss_elements and gff_gene in new_feature_dict['TSS']:
					feature_pos_dict[gff_gene]['TSS'] = gff_pos
				gene_children = []
			
			# Add the exon related elements
			elif gff_feature_type == 'exon':
				for parent_id in assignGffParents(gff_entry_list[8]):
					if add_introns and parent_id in new_feature_dict['intron']:
						feature_pos_dict[parent_id]['intron'] = gff_pos
						if not merged_gff: continue
						for sibling_id in gene_children:
							if sibling_id == parent_id: continue
							if sibling_id not in new_feature_dict['intron']: continue
							feature_pos_dict[sibling_id]['intron'] = gff_pos

			# Add the CDS related elements
			elif gff_feature_type == 'CDS':
				for parent_id in assignGffParents(gff_entry_list[8]):
					if add_utrs and parent_id in new_feature_dict['5prime_UTR'] and not feature_pos_dict[parent_id]['5prime_UTR']: 
						feature_pos_dict[parent_id]['5prime_UTR'] = gff_pos - 1
					if add_utrs and parent_id in new_feature_dict['3prime_UTR']:
						feature_pos_dict[parent_id]['3prime_UTR'] = gff_pos

			elif 'Parent' in gff_entry_list[8] and assignGffParent(gff_entry_list[8]) == gff_gene: gene_children.append(assignGffID(gff_entry_list[8]))

	# Convert the feature dict to a position-based dict
	pos_feature_dict = defaultdict(str)
	for parent_id, feature_dict in feature_pos_dict.items():
		for feature_type, feature_pos in feature_dict.items():
			if pos_feature_dict[feature_pos]: pos_feature_dict[feature_pos] += '\n'
			pos_feature_dict[feature_pos] += new_feature_dict[feature_type][parent_id]

	# Create the output gff
	gff_output_file = open(gff_output, 'w') 
	with open(gff_input, 'r') as gff_data:
		for gff_pos, gff_line in enumerate(gff_data):
			if gff_line.startswith('#'): gff_output_file.write(gff_line)
			else:
				gff_entry_list = gff_line.split('\t')
				gff_feature_type = gff_entry_list[2]
				if gff_feature_type in ['intron', 'first_intron']: 
					if not add_introns: gff_output_file.write(gff_line)
				elif gff_feature_type in ['tss_upstream', 'tss_flanks']: 
					if not add_tss_elements: gff_output_file.write(gff_line)
				elif gff_feature_type in ['five_prime_UTR', 'three_prime_UTR']:
					if not add_utrs: gff_output_file.write(gff_line)
				else: gff_output_file.write(gff_line)
			if gff_pos in pos_feature_dict: 
				gff_output_file.write(pos_feature_dict[gff_pos] + '\n')
	gff_output_file.close()

def mergeItervals (intervals_to_merge):
	'''
	Using reduce, run only when at least a single interval is 
	stored. If so, check if the new interval overlaps the last 
	stored interval. If they overlap, combine the intervals 
	and start another iteration. When reduced, return the merged
	intervals.
	'''
	def merge (intervals, interval):
		if intervals:
			if intervals[-1][1] >= interval[0]:
				intervals[-1] = intervals[-1][0], max(intervals[-1][1], interval[1])
				return intervals
		intervals.append(interval)
		return intervals

	return reduce(merge, sorted(intervals_to_merge), [])

def subtractItervals (intervals_to_subtract, subtract_intervals):
	'''
	Loops two interval lists, subtracts the intervals within
	`subtract_intervals` from `intervals_to_subtract`. 
	`intervals_to_subtract` will be updated to include a list
	of non-overlapping intervals.

	For example:

	A = intervals_to_subtract (intervals)
	B = subtract_intervals
	C = intervals_to_subtract (non-overlapping intervals)

	A: *-------------*   *---------------------*
	B: *-----*   *------------*      *---------*
	C:        *-*              *----*

	'''
	def subtractIterval (interval_to_subtract, subtract_interval):
		# Return interval_to_subtract, if no overlap
		if not (interval_to_subtract[0] <= subtract_interval[1] and subtract_interval[0] <= interval_to_subtract[1]):
			return [interval_to_subtract]
		
		# If overlap, subtract subtract_interval
		sorted_positions = sorted(itertools.chain(interval_to_subtract, subtract_interval))
		updated_itervals = []
		if sorted_positions[0] == interval_to_subtract[0] and sorted_positions[0] != sorted_positions[1]: 
			updated_itervals.append((sorted_positions[0], sorted_positions[1] - 1))
		if sorted_positions[3] == interval_to_subtract[1] and sorted_positions[2] != sorted_positions[3]:
			updated_itervals.append((sorted_positions[2] + 1, sorted_positions[3]))
		return updated_itervals

	for subtract_interval in subtract_intervals:
		intervals_to_subtract = list(itertools.chain(*[subtractIterval(interval_to_subtract, subtract_interval) for interval_to_subtract in intervals_to_subtract]))

	return intervals_to_subtract

def intersectItervals (intervals_to_intersect):
	'''
	Loops interval list, finds the overlap between the given
	intervals. All intervals must overlap

	For example:

	A = Interval A
	B = Interval B
	C = Intersection between A and B

	A: *------------------------------*
	B:           *-----------------------------*
	C:           *--------------------*

	'''

	# Assign initial start and end values for the intersection interval
	intersect_start, interset_end = intervals_to_intersect[0]

	# Loop the other intervals
	for interval_start, interval_end in intervals_to_intersect[1:]:

		# Check if the current start position is greater than the stored value
		if interval_start > intersect_start: intersect_start = interval_start

		# Check if the current end position is less than the stored value
		if interval_end < interset_end: interset_end = interval_end

	# Return the intersection interval
	return intersect_start, interset_end

def returnItervalOverlaps (intervals, overlap_interval):
	'''
	Returns the intervals within `intervals` that overlap
	with overlap_interval.

	For example:

	A = intervals (non-overlapping intervals)
	B = overlap_interval
	C = intervals (overlapping intervals)

	A: *-------------*   *---------------------*
	B:           *-----*
	C: *-------------*
	'''
	intervals_that_overlap = []
	for interval in intervals:
		if interval[0] <= overlap_interval[1] and overlap_interval[0] <= interval[1]: 
			intervals_that_overlap.append(interval) 
	return intervals_that_overlap

def countIterval (interval):
	'''
	Sum the interval length.
	'''

	return (interval[1] + 1) - interval[0]

def countItervals (intervals, feature_type, count_dict):
	'''
	Sums the interval lengths within `intervals`. Adds
	the sum to the supplied count_dict for the specified
	feature type.
	'''
	for interval in intervals: count_dict[feature_type] += countIterval(interval)
	return count_dict

def assignPrime5 (size, gene_data, gene_interval, prime3_limit):
	'''
	Adds an interval to the 5` of a gene. The function uses
	the chromosome length (i.e. prime3_limit) to make sure
	no interval goes outside the chromosome. The function
	returns the updated gene interval - i.e. which includes
	the 5` interval - and the 5` interval itself.
	'''
	if gene_data.strand == '+': 
		prime5_interval = [gene_data.start - 1, gene_data.start - 1]
		prime5_interval[0] = max(1, gene_data.start - size)
		gene_interval[0] = min(gene_interval[0], prime5_interval[0])
	if gene_data.strand == '-':
		prime5_interval = [gene_data.end + 1, gene_data.end + 1]
		prime5_interval[1] = min(prime3_limit, (gene_data.end + size))
		gene_interval[1] = max(gene_interval[1], prime5_interval[1])
	return gene_interval, tuple(prime5_interval)

def assignPrime3 (size, gene_data, gene_interval, prime3_limit):
	'''
	Adds an interval to the 3` of a gene. The function uses
	the chromosome length (i.e. prime3_limit) to make sure
	no interval goes outside the chromosome. The function
	returns the updated gene interval - i.e. which includes
	the 3` interval - and the 3` interval itself.
	'''
	if gene_data.strand == '+':
		prime3_interval = [gene_data.end + 1, gene_data.end + 1]
		prime3_interval[1] = min(prime3_limit,  (gene_data.end + size))
		gene_interval[1] = max(gene_interval[1], prime3_interval[1])
	if gene_data.strand == '-':
		prime3_interval = [gene_data.start - 1, gene_data.start - 1]
		prime3_interval[0] = max(1, gene_data.start - size)
		gene_interval[0] = min(gene_interval[0], prime3_interval[0])
	return gene_interval, tuple(prime3_interval)

def createIntrons (gff_db, gene, label_first_intron):
	'''
	Iterate the mRNA by gene. For each mRNA, read in the exon
	features. Generate the intron intervals by subtracting the
	entire mRNA length by the exon intervals. Once the intron 
	intervals are created, generate the intron GFF entires.
	'''

	# Create a dict to store the intron gff entires by mRNA
	intron_str_dict = {}

	# Create a list of the exon features, if there is more than
	# exon, assign the exon intervals.
	mRNA_features = gff_db.children(gene, featuretype = ['mRNA', 'transcript'])
	for mRNA in mRNA_features:
		exon_features = list(gff_db.children(mRNA, featuretype = 'exon'))

		if len(exon_features) < 2: continue
		exon_intervals = []
		for exon_feature in exon_features: exon_intervals.append((exon_feature.start, exon_feature.end))
		
		# Assign an intron interval for the entire mRNA, then 
		# subtract the exon intervals from the single intron 
		# interval to create the intron intervals.
		intron_intervals = [(mRNA.start, mRNA.end)]
		intron_intervals = subtractItervals(intron_intervals, exon_intervals)

		# Confirm the order of the intervals
		if mRNA.strand == '-': intron_intervals = sorted(intron_intervals, key = lambda x: x[0], reverse = True)

		# Create the intron GFF entires for the mRNA, if the
		# mRNA is on the reverse strand. Reverse the intron
		# GFF entires before assignment.
		intron_str_list = []
		for intron_pos, (intron_start, intron_end) in enumerate(intron_intervals):
			if label_first_intron and intron_pos == 0: intron_str_list.append(f'{mRNA.seqid}\tgffADD\tfirst_intron\t{intron_start}\t{intron_end}\t.\t{mRNA.strand}\t.\tID={mRNA.id}:intron_{intron_pos + 1};Parent={mRNA.id};')
			else: intron_str_list.append(f'{mRNA.seqid}\tgffADD\tintron\t{intron_start}\t{intron_end}\t.\t{mRNA.strand}\t.\tID={mRNA.id}:intron_{intron_pos + 1};Parent={mRNA.id};')
		if mRNA.strand == '-': intron_str_list.reverse()
		if intron_str_list: intron_str_dict[mRNA.id] = '\n'.join(intron_str_list)

	# Return the dict
	return intron_str_dict

def createUTRs (gff_db, gene):
	'''
	Iterate the mRNA by gene. For each mRNA, read in the exon
	and cds features. Generate the 5'/3' UTR intervals by 
	subtracting the cds intervals from the exon intervals. Once 
	the UTR	intervals are created, generate the UTR GFF entires.
	'''

	def assign5UTR (exon_intervals, cds_intervals, strand, id):
		'''
		Assign the 5prime UTR interval(s)
		'''

		'''
		Sort all the intervals. Reverse the intervals if they
		are from the reverse/negative strand.
		'''
		if strand == '+': 
			exon_intervals.sort()
			cds_intervals.sort()
		elif strand == '-':
			exon_intervals.sort(reverse = True)
			cds_intervals.sort(reverse = True)
		else: raise (f'Unable to assign strand: {strand}')

		'''
		Loop the exon intervals. For each exon interval
		subtract from the CDS intervals.
		Check four possible outcomes:
		1) Halt if nothing is returned
		2) Return error if three intervals are returned
		3) Assign the relevant interval if two intervals are 
		   returned
		4) Lastely, assign the returned interval (if only one)
		'''
		utr5_intervals = []
		for exon_interval in exon_intervals:
			exon_utr5_interval = subtractItervals([exon_interval], cds_intervals)
			if not exon_utr5_interval: break
			if len(exon_utr5_interval) > 2: raise Exception(f'Unexpected UTR intervals: {exon_utr5_interval}')
			elif len(exon_utr5_interval) == 2 and strand == '-': exon_utr5_interval = exon_utr5_interval[1]
			elif len(exon_utr5_interval) == 2 and strand == '+': exon_utr5_interval = exon_utr5_interval[0]
			else: exon_utr5_interval = exon_utr5_interval[0]

			'''
			Confirm the interval is not the 3prime UTR, if not
			append the interval to the 5prime UTR interval list
			'''
			sorted_positions = sorted(itertools.chain(exon_interval, exon_utr5_interval))
			if strand == '-' and sorted_positions[2] != sorted_positions[3]: break
			elif strand == '+' and sorted_positions[0] != sorted_positions[1]: break
			else: utr5_intervals.append(exon_utr5_interval)

		return utr5_intervals

	def assign3UTR (exon_intervals, cds_intervals, strand, id):
		'''
		Assign the 5prime UTR interval(s)
		'''

		'''
		Sort all the intervals. Reverse the intervals if they
		are from the forward/positve strand.
		'''
		if strand == '+': 
			exon_intervals.sort(reverse = True)
			cds_intervals.sort(reverse = True)
		elif strand == '-':
			exon_intervals.sort()
			cds_intervals.sort()
		else: raise (f'Unable to assign strand: {strand}')

		'''
		Loop the exon intervals. For each exon interval
		subtract from the CDS intervals.
		Check four possible outcomes:
		1) Halt if nothing is returned
		2) Return error if three intervals are returned
		3) Assign the relevant interval if two intervals are 
		   returned
		4) Lastely, assign the returned interval (if only one)
		'''
		utr3_intervals = []
		for exon_interval in exon_intervals:
			exon_utr3_interval = subtractItervals([exon_interval], cds_intervals)
			if not exon_utr3_interval: break
			if len(exon_utr3_interval) > 2: raise Exception(f'Unexpected UTR intervals: {exon_utr3_interval}')
			elif len(exon_utr3_interval) == 2 and strand == '-': exon_utr3_interval = exon_utr3_interval[0]
			elif len(exon_utr3_interval) == 2 and strand == '+': exon_utr3_interval = exon_utr3_interval[1]
			else: exon_utr3_interval = exon_utr3_interval[0]
			
			'''
			Confirm the interval is not the 5prime UTR, if not
			append the interval to the 3prime UTR interval list
			'''
			sorted_positions = sorted(itertools.chain(exon_interval, exon_utr3_interval))
			if strand == '+' and sorted_positions[2] != sorted_positions[3]: break
			elif strand == '-' and sorted_positions[0] != sorted_positions[1]: break
			else: utr3_intervals.append(exon_utr3_interval)
		return utr3_intervals

	# Create a dict to store the UTR gff entires by mRNA
	utr_5_str_dict = {}
	utr_3_str_dict = {}

	'''
	Create a list of the exon and cds features, then assign
	their intervals.
	'''
	mRNA_features = gff_db.children(gene, featuretype = ['mRNA', 'transcript'])
	for mRNA in mRNA_features:
		exon_features = list(gff_db.children(mRNA, featuretype = 'exon'))
		cds_features = list(gff_db.children(mRNA, featuretype = 'CDS'))
		exon_intervals = []
		cds_intervals = []
		for exon_feature in exon_features: exon_intervals.append((exon_feature.start, exon_feature.end))
		for cds_feature in cds_features: cds_intervals.append((cds_feature.start, cds_feature.end))

		'''
		Assign the 3prime/5prime intervals. Once assigned
		reverse the 3prime intervals.
		'''
		utr_5_intervals = assign5UTR(exon_intervals, cds_intervals, mRNA.strand, mRNA.id)
		utr_3_intervals = assign3UTR(exon_intervals, cds_intervals, mRNA.strand, mRNA.id)
		utr_3_intervals.reverse()

		'''
		Create the 5prime UTR GFF entires for the mRNA, if 
		the mRNA is on the reverse strand. Reverse the UTR
		GFF entires before assignment.
		'''
		utr_5_str_list = []
		for utr_5_pos, (utr_5_start, utr_5_end) in enumerate(utr_5_intervals):
			utr_5_str_list.append(f'{mRNA.seqid}\tgffADD\tfive_prime_UTR\t{utr_5_start}\t{utr_5_end}\t.\t{mRNA.strand}\t.\tID={mRNA.id}:five_prime_utr_{utr_5_pos + 1};Parent={mRNA.id};')
		if mRNA.strand == '-': utr_5_str_list.reverse()
		if utr_5_str_list: utr_5_str_dict[mRNA.id] = '\n'.join(utr_5_str_list)

		'''
		Create the 3prime UTR GFF entires for the mRNA, if 
		the mRNA is on the reverse strand. Reverse the UTR
		GFF entires before assignment.
		'''
		utr_3_str_list = []
		for utr_3_pos, (utr_3_start, utr_3_end) in enumerate(utr_3_intervals):
			utr_3_str_list.append(f'{mRNA.seqid}\tgffADD\tthree_prime_UTR\t{utr_3_start}\t{utr_3_end}\t.\t{mRNA.strand}\t.\tID={mRNA.id}:three_prime_utr_{utr_3_pos + 1};Parent={mRNA.id};')
		if mRNA.strand == '-': utr_3_str_list.reverse()
		if utr_3_str_list: utr_3_str_dict[mRNA.id] = '\n'.join(utr_3_str_list)

	# Return the dicts
	return utr_5_str_dict, utr_3_str_dict

def createTSSElements (gff_db, gene, chrom_size, tss_upstream_bp, tss_flanks_bp):
	'''
	Assign the TSS for the current gene using the start, stop
	and strand of the gene.
	'''

	def createTSSPrime5 (size, tss_pos, strand, prime3_limit):
		'''
		Creates the 5` TSS element. The function uses the 
		chromosome length (i.e. prime3_limit) to make sure
		no interval goes outside the chromosome.
		'''
		if strand == '+': 
			prime5_interval = [tss_pos - 1, tss_pos - 1]
			prime5_interval[0] = max(1, tss_pos - size)
		if strand == '-':
			prime5_interval = [tss_pos + 1, tss_pos + 1]
			prime5_interval[1] = min(prime3_limit, (tss_pos + size))
		return tuple(prime5_interval)

	def createTSSFlanks (size, tss_pos, prime3_limit):
		'''
		Creates the 5`/3` flanking TSS element.. The function 
		uses the chromosome length (i.e. prime3_limit) to make 
		sure no interval goes outside the chromosome.
		'''
		flank_interval = [tss_pos - 1, tss_pos + 1]
		flank_interval[0] = max(1, tss_pos - size)
		flank_interval[1] = min(prime3_limit,  (tss_pos + size))
		return tuple(flank_interval)

	# Create a dict to store the tss gff entires by mRNA
	tss_str_dict = {}

	# Assign the TSS using the strand
	try:
		if gene.strand == '+': tss_start_pos = gene.start
		elif gene.strand == '-': tss_start_pos = gene.stop
		else: raise Exception('Unable to assign TSS using the strand')
	except: raise Exception('Unable to assign TSS')

	# Assign the TSS elements
	tss5_start, tss5_end = createTSSPrime5(tss_upstream_bp, tss_start_pos, gene.strand, chrom_size)
	tssFlank_start, tssFlank_end = createTSSFlanks(tss_flanks_bp, tss_start_pos, chrom_size)

	'''
	Create the intron GFF entires for the mRNA, if the
	mRNA is on the reverse strand. Reverse the intron
	GFF entires before assignment.
	'''
	tss_str_list = []
	tss_str_list.append(f'{gene.seqid}\tgffADD\ttss_upstream\t{tss5_start}\t{tss5_end}\t.\t{gene.strand}\t.\tID={gene.id}:tss_upstream;Parent={gene.id};')
	tss_str_list.append(f'{gene.seqid}\tgffADD\ttss_flanks\t{tssFlank_start}\t{tssFlank_end}\t.\t{gene.strand}\t.\tID={gene.id}:tss_flanks;Parent={gene.id};')
	if tss_str_list: tss_str_dict[gene.id] = '\n'.join(tss_str_list)

	# Return the dict
	return tss_str_dict

def chromCounts (gff_db, chrom_name, chrom_limit, feature_count_dict, feature_set, prioritize = False, priority_order = None, promoter_bp = None, upstream_bp = None, downstream_bp = None, **kwargs):
	'''
	Iterate the chromosome by position. As most of the chromosome
	is itergentic, only genes (and overlaps, if present) need to
	checked. The chromosome_pos variable is used to keep track of
	the number of itergentic sites and allows for bulk counting.

	As gene (and overlap, if present) features may overlap, the
	intervals of these features are merged. The merging process
	removes erroneous (and redundant) counting. These merged
	intervals are then scanned for features.

	The intervals for each feature type are then merged, to again
	remove erroneous (and redundant) counting. The merged features
	are then used to determine the intergenic intervals, if
	applicable. 

	If no priority is given, the features found at each position
	will be recorded. Otherwise, only the feature with the top 
	priority is recorded.

	'''

	# Assign the chromosome starting position
	chromosome_pos = 1

	'''
	Create a list to store the gene features. Also create a dict
	to store added feature - i.e. features not found within the
	GFF file.
	'''
	gene_intervals = []
	added_feature_dict = defaultdict(list)

	'''
	Loop the gene features. Assign the additional features, if
	the relevant size is specified by the user. Update the gene
	intervals to include the additional features.
	'''
	for gene in gff_db.features_of_type('gene', limit=f'{chrom_name}:1-{chrom_limit}'):
		gene_interval = [copy.deepcopy(gene.start), copy.deepcopy(gene.end)]
		if promoter_bp:
			gene_interval, prime5_interval = assignPrime5(promoter_bp, gene, gene_interval, chrom_limit)
			added_feature_dict['promoter'].append(prime5_interval)
		if upstream_bp:
			gene_interval, prime5_interval = assignPrime5(upstream_bp, gene, gene_interval, chrom_limit)
			added_feature_dict['upstream'].append(prime5_interval)
		if downstream_bp:
			gene_interval, prime3_interval = assignPrime3(downstream_bp, gene, gene_interval, chrom_limit)
			added_feature_dict['downstream'].append(prime3_interval)
		gene_intervals.append(tuple(gene_interval))

	# Merge the intervals
	merged_intervals = mergeItervals(gene_intervals)

	# Loop the merged intervals
	for merged_interval_start, merged_interval_end in merged_intervals:

		# Bulk count the intergenic sites, create the intergenic_interval
		if chromosome_pos < merged_interval_start: feature_count_dict['intergenic'] += (merged_interval_start - chromosome_pos)
		chromosome_pos = (merged_interval_end + 1)
		intergenic_interval = [(merged_interval_start, merged_interval_end)]

		'''
		Create a dict to store the feature intervals. Once stored, merge 
		them by type (i.e. exon, intron, etc.)
		'''
		feature_dict = defaultdict(list)
		features = gff_db.region(seqid = chrom_name, start = merged_interval_start, end = merged_interval_end + 1, featuretype = feature_set)
		if promoter_bp: feature_dict['promoter'] = returnItervalOverlaps(added_feature_dict['promoter'], (merged_interval_start, merged_interval_end))
		if upstream_bp: feature_dict['upstream'] = returnItervalOverlaps(added_feature_dict['upstream'], (merged_interval_start, merged_interval_end))
		if downstream_bp: feature_dict['downstream'] = returnItervalOverlaps(added_feature_dict['downstream'], (merged_interval_start, merged_interval_end))
		for feature in features: feature_dict[feature.featuretype].append((feature.start, feature.end))
		for feature in feature_dict.keys(): feature_dict[feature] = mergeItervals(feature_dict[feature])

		# Check if the intervals should be prioritized
		if prioritize:

			# Assign feature priorty, only using found feature.
			feature_priority = [_fp for _fp in priority_order if _fp in list(feature_dict)]

			'''
			Move down the priority list. Merge the intervals from the previous 
			level(s) and subtract from the intervals of the current level.
			'''
			for feature_pos in range(1, len(feature_priority)):
				higher_priority_intervals = list(itertools.chain.from_iterable(feature_dict[_fstr] for _fstr in feature_priority[0:feature_pos]))
				feature_dict[feature_priority[feature_pos]] = subtractItervals(feature_dict[feature_priority[feature_pos]], higher_priority_intervals)

		'''
		Count the intervals within the interval dict, then calculate and
		count the intergenic intervals. When counting intergenic intervals
		do not subtract the promoter, upstream, or downstream.
		'''
		for feature in feature_dict.keys(): feature_count_dict = countItervals(feature_dict[feature], feature, feature_count_dict)
		for feature in feature_dict.keys():
			if feature in ['promoter', 'upstream', 'downstream', 'tss_flanks', 'tss_upstream'] and not prioritize: continue
			intergenic_interval = subtractItervals(intergenic_interval, feature_dict[feature])
		feature_count_dict = countItervals(intergenic_interval, 'intergenic', feature_count_dict)

	# Bulk count the intergenic sites at the 3', if needed
	if chromosome_pos <= chrom_limit: feature_count_dict['intergenic'] += ((chrom_limit + 1) - chromosome_pos)
	return feature_count_dict

def posCounts (gff_db, chrom_name, chrom_limit, feature_count_dict, chrom_position_list, feature_set, prioritize = False, priority_order = None, promoter_bp = None, upstream_bp = None, downstream_bp = None, **kwargs):
	'''
	Iterate the chromosome. As most of the chromosome is
	intergenic, only genes (and overlaps, if present) need to
	checked. 

	As gene (and overlap, if present) features may overlap, the
	intervals of these features are merged. The merging process
	removes erroneous (and redundant) counting. These merged
	intervals are then scanned for features.

	The intervals for each feature type are then merged, to again
	remove erroneous (and redundant) counting. The merged features
	are then used to determine the intergenic intervals, if
	applicable. 

	If no priority is given, the features found at each position
	within `chrom_position_list` will be recorded. Otherwise, 
	only the feature with the top priority is recorded. Lastly,
	the genes associated with each position/feature are recorded. 
	'''

	def returnParents (feature):
		
		# Assign parent(s) of the feature
		parent_gene = ''
		parent_other = []
		for parent_feature in gff_db.parents(feature):
			if parent_feature.featuretype == 'gene':
				if 'Name' in parent_feature.attributes: parent_gene = parent_feature.attributes['Name'][0]
				else: parent_gene = parent_feature.id
			else:
				if 'Name' in parent_feature.attributes: parent_other.append(parent_feature.attributes['Name'][0])
				else: parent_other.append(parent_feature.id)

		# Return gene and sub-parent (if found)
		if parent_other: return f'{parent_gene}({",".join(parent_other)})'
		else: return parent_gene

	def yieldPositions (position_list):
		for _cpos in position_list:
			if isinstance(_cpos, np.int64): yield (_cpos, _cpos)
			elif isinstance(_cpos, np.ndarray): yield (_cpos[0], _cpos[1])
			else: raise Exception(f'Unable to yield from type({type(_cpos)}) with value: {_cpos}')

	'''
	Assign a dict to store the features and genes for each 
	position
	'''
	position_feature_dict = defaultdict(list)

	'''
	Create a list to store the gene features. Also create a dict
	to store added feature - i.e. features not found within the
	GFF file.
	'''
	gene_intervals = []
	added_feature_dict = defaultdict(lambda: defaultdict(list))

	'''
	Loop the gene features. Assign the additional features, if
	the relevant size is specified by the user. Update the gene
	intervals to include the additional features.
	'''
	for gene in gff_db.features_of_type(['gene', 'rRNA', 'tRNA'], limit=f'{chrom_name}:1-{chrom_limit}'):
		gene_interval = [copy.deepcopy(gene.start), copy.deepcopy(gene.end)]

		# Assign the gene id for the current gene
		if 'Name' in gene.attributes: gene_id = gene.attributes['Name']
		elif 'gene' in gene.attributes: gene_id = gene.attributes['gene']
		if len(gene_id) > 1: raise Exception('GFF malformed. Unable to assign gene id')
		else: gene_id = gene_id[0]

		# Assign the intervals for each gene
		if promoter_bp:
			gene_interval, prime5_interval = assignPrime5(promoter_bp, gene, gene_interval, chrom_limit)
			added_feature_dict['promoter'][gene_id].append(prime5_interval)
		if upstream_bp:
			gene_interval, prime5_interval = assignPrime5(upstream_bp, gene, gene_interval, chrom_limit)
			added_feature_dict['upstream'][gene_id].append(prime5_interval)
		if downstream_bp:
			gene_interval, prime3_interval = assignPrime3(downstream_bp, gene, gene_interval, chrom_limit)
			added_feature_dict['downstream'][gene_id].append(prime3_interval)
		gene_intervals.append(tuple(gene_interval))

	# Merge the intervals
	merged_intervals = mergeItervals(gene_intervals)

	# Loop the merged intervals
	for merged_interval_start, merged_interval_end in merged_intervals:

		'''
		Assign the relevant positions - i.e. positions within the
		merged interval.
		'''
		relevant_positions = []
		for chrom_position_start, chrom_position_stop in yieldPositions(chrom_position_list):
			if chrom_position_start <= merged_interval_end and merged_interval_start <= chrom_position_stop: relevant_positions.append((chrom_position_start, chrom_position_stop))

		# Assign the starting intergenic interval
		intergenic_interval = [(merged_interval_start, merged_interval_end)]

		'''
		Create a dict to store the feature intervals. Once 
		stored, merge them by type (i.e. exon, intron, etc.)
		'''
		feature_dict = defaultdict(list)
		features = list(gff_db.region(seqid = chrom_name, start = merged_interval_start, end = merged_interval_end + 1, featuretype = feature_set))
		if promoter_bp: feature_dict['promoter'] = returnItervalOverlaps(itertools.chain.from_iterable(added_feature_dict['promoter'].values()), (merged_interval_start, merged_interval_end))
		if upstream_bp: feature_dict['upstream'] = returnItervalOverlaps(itertools.chain.from_iterable(added_feature_dict['upstream'].values()), (merged_interval_start, merged_interval_end))
		if downstream_bp: feature_dict['downstream'] = returnItervalOverlaps(itertools.chain.from_iterable(added_feature_dict['downstream'].values()), (merged_interval_start, merged_interval_end))
		for _f in features: feature_dict[_f.featuretype].append((_f.start, _f.end))
		for _f in feature_dict.keys(): feature_dict[_f] = mergeItervals(feature_dict[_f])
		
		# Check if the intervals should be prioritized
		if prioritize:

			# Assign feature priorty, only using found feature.
			feature_priority = [_fp for _fp in priority_order if _fp in list(feature_dict)]

			'''
			Move down the priority list. Merge the intervals 
			from the previous level(s) and subtract from the 
			intervals of the current level.
			'''
			for feature_pos in range(1, len(feature_priority)):
				higher_priority_intervals = list(itertools.chain.from_iterable(feature_dict[_fstr] for _fstr in feature_priority[0:feature_pos]))
				feature_dict[feature_priority[feature_pos]] = subtractItervals(feature_dict[feature_priority[feature_pos]], higher_priority_intervals)

		# Calculate and the intergenic intervals
		for feature in feature_dict.keys():
			if feature in ['promoter', 'upstream', 'downstream'] and not prioritize: continue
			intergenic_interval = subtractItervals(intergenic_interval, feature_dict[feature])
		feature_dict['intergenic'] = intergenic_interval

		'''
		Assign the features and genes for each relevant 
		position
		'''
		for feature_type, feature_intervals in feature_dict.items():
			for feature_interval_start, feature_interval_end in feature_intervals:
				for relevant_position_start, relevant_position_stop in relevant_positions:
					if relevant_position_start <= feature_interval_end and feature_interval_start <= relevant_position_stop:
						
						# Count added features - i.e. not in the GFF
						if feature_type in list(added_feature_dict):
							for gene_id, added_feature_intervals, in added_feature_dict[feature_type].items():
								for added_feature_start, added_feature_end in added_feature_intervals:
									if relevant_position_start <= added_feature_end and added_feature_start <= relevant_position_stop:
										position_feature_dict[(relevant_position_start, relevant_position_stop)].append([feature_type, gene_id])
						
						# Count features within the GFF
						else:
							for feature in features:
								if feature.featuretype != feature_type: continue
								if relevant_position_start <= feature.end and feature.start <= relevant_position_stop:

									'''
									Assign features based on their type/attributes.
									1) For mRNA - return mRNA & gene
									2) For Exons - return exon & mRNA
									3) For TSS(up/down) - return TSS & gene
									4) If unkown - report error

									'''
									if 'transcript_id' in feature.attributes: position_feature_dict[(relevant_position_start, relevant_position_stop)].append([feature_type, returnParents(feature)])
									elif 'Parent' in feature.attributes: position_feature_dict[(relevant_position_start, relevant_position_stop)].append([feature_type, returnParents(feature)])
									elif feature_type in ['tss_flanks', 'tss_upstream']:
										position_feature_dict[(relevant_position_start, relevant_position_stop)].append([feature_type, feature.attributes['ID'][0].split(':')[0]])
									else: raise Exception(f'Unable to assign {feature_type} with attributes {feature.attributes}')

		# Count the features for each relevant interval
		for relevant_position_start, relevant_position_stop in relevant_positions:

			# Loop the features and confirm they overlap the relevant interval
			for feature_type, feature_intervals in feature_dict.items():			
				for feature_interval_start, feature_interval_end in feature_intervals:
					if relevant_position_start <= feature_interval_end and feature_interval_start <= relevant_position_stop:

						'''
						Create a intersection of the feature and relevant interval.
						Then add the length of the intersection interval to the dict.
						'''
						intersection_interval = intersectItervals([[relevant_position_start, relevant_position_stop], [feature_interval_start, feature_interval_end]])
						feature_count_dict = countItervals([intersection_interval], feature_type, feature_count_dict)

	'''
	Lastly, assign the intergenic feature for all positions 
	outside the merged intervals and return the feature dict
	'''
	for chrom_position in yieldPositions(chrom_position_list):
		if chrom_position not in position_feature_dict:
			position_feature_dict[chrom_position].append(['intergenic', ''])
			feature_count_dict = countItervals([chrom_position], 'intergenic', feature_count_dict)
	
	return position_feature_dict, feature_count_dict
