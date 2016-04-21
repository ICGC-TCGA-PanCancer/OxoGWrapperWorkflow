use strict;
use String::Random;
use Getopt::Long;
# this creates a merge VCF by variant type e.g. SNV, indel, SV
# this produces a very simple VCF and also handles merging multiple variants
# into a single line

my $info = {};
my $d = {};


# Inputs: the filenames for SNVs for Broad, Sanger, DKFZ/EMBL;
# the filenames for INDELs for Broad, Sanger, DKFZ/EMBL;
# the filenames for SVs for Broad, Sanger, DKFZ/EMBL;
# the path to the root directory where all the files are;
# the path to the output directory.

# Call this script like this:
# perl vcf_merge_by_type.pl --broad_snv <broad SNV filename> \
#                           --sanger_snv <sanger SNV filename> \
#                           --de_snv <DKFZ/EMBL SNV filename> \
#                           --muse_snv <MUSE SNV filename> \
#                           --broad_sv <broad SV filename> \
#                           --sanger_sv <sanger SV filename> \
#                           --de_sv <DKFZ/EMBL SV filename> \
#                           --broad_indel <broad INDEL filename> \
#                           --sanger_indel <sanger INDEL filename> \
#                           --de_indel <DKFZ/EMBL INDEL filename> \
#                           --indir /datastore/path_to_above_VCFs/ \
#                           --outdir /datastore/output_directory 

my ($broad_snv, $sanger_snv, $de_snv, $muse_snv,
        $broad_indel, $sanger_indel, $de_indel,
        $broad_sv, $sanger_sv, $de_sv,
        $in_dir, $out_dir); # = @ARGV;

GetOptions ("broad_snv=s" => \$broad_snv,
			"sanger_snv=s" => \$sanger_snv,
			"dkfz_embl_snv=s" => \$de_snv,
			"muse_snv=s" => \$muse_snv,
			"broad_sv=s" => \$broad_sv,
			"sanger_sv=s" => \$sanger_sv,
			"dkfz_embl_sv=s" => \$de_sv,
			"broad_indel=s" => \$broad_indel,
			"sanger_indel=s" => \$sanger_indel,
			"dkfz_embl_indel=s" => \$de_indel,
			"indir=s" => \$in_dir,
			"outdir=s" => \$out_dir);

my @snv = ($broad_snv, $sanger_snv, $de_snv, $muse_snv);
my @indel = ($broad_indel, $sanger_indel, $de_indel);
my @sv = ($broad_sv, $sanger_sv, $de_sv);

process($out_dir."/snv.clean", @snv);
process($out_dir."/indel.clean", @indel);
process($out_dir."/sv.clean", @sv);

sub process {

  my $workflow = shift;
  my @files = @_;

  $d = {};

  $info = {};
  my $header = <<"EOS";
##fileformat=VCFv4.1
##variant_merge=$workflow
#CHROM  POS     ID      REF     ALT     QUAL    FILTER  INFO
EOS

  open my $OUT, ">$workflow.vcf" or die;

  print $OUT $header;

  # process file into hash
  foreach my $i (@files) {
  	if ((defined $i) && !($i eq ""))
  	{
	    print "processing file $i\n";
	    process_file($in_dir."/".$i, $OUT);
  	}
  }

  # write hash
  foreach my $chr (sort keys %{$d}) {
    foreach my $pos (sort keys %{$d->{$chr}}) {
      print $OUT $d->{$chr}{$pos}."\n";
    }
  }
  close $OUT;

  sort_and_index($workflow);

}

sub sort_and_index {

  my ($file) = @_;
  my @parts = split /\//, $file;
  my $filename = $parts[-1];
  my $rnd = new String::Random;
  my $randomString = $rnd->randregex('\w{16}');
  my $cmd = "sudo docker run --rm --name=sort_merged_vcf_$randomString \\
        -v $file.vcf:/input.vcf:rw \\
        -v /datastore/refdata/public:/ref \\
        -v $out_dir:/outdir/:rw \\
        compbio/ngseasy-base:a1.0-002 /bin/bash -c \\
        \" vcf-sort /input.vcf > /outdir/$filename.sorted.vcf; \\
        echo zipping_and_indexing ; \\
        bgzip -f -c /outdir/$filename.sorted.vcf > /outdir/$filename.sorted.vcf.gz ; \\
        tabix -p vcf /outdir/$filename.sorted.vcf.gz\"
   ";

  print "$cmd\n";

  my $result = system($cmd);

  print "Status of sort: $result\n";
}


#Is this sub even used anymore? If not, it should be removed.
sub parse_info {
  my ($file, $info_hash) = @_;
  open(IN, "zcat $file |") or die;
  while(<IN>) {
    chomp;
    if (/^##INFO=(.+)$/) {
      $info_hash->{$1} = 1;
    }
  }
  close IN;
}

sub process_file {
  my ($file, $OUT) = @_;
  open(IN, "zcat $file |") or die;
  while(<IN>) {
    chomp;
    next if (/^#/);
    # FIXME
    # next if (!/PASS/ || /tier/);
    my @a = split /\t/;
    my $payload = "$a[0]\t$a[1]\t.\t$a[3]\t$a[4]\t$a[5]\t$a[6]\t.";
    $d->{$a[0]}{$a[1]} = $payload;
  }
  close IN;
}

