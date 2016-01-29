use strict;

# this creates a merge VCF by variant type e.g. SNV, indel, SV
# this produces a very simple VCF and also handles merging multiple variants
# into a single line

my $info = {};
my $d = {};

# TODO: parameterize file inputs! 
#my @snv = ('broad-brian/a76d7d7a-6f19-4ae9-a152-7b909130946c/35a74e53-16ff-4764-8397-6a9b02dfe733.broad-mutect.20151216.somatic.snv_mnv.vcf.gz',
#'sanger-brian/0bac6371-e554-44b6-acb1-64e452db583f/35a74e53-16ff-4764-8397-6a9b02dfe733.svcp_1-0-5.20150707.somatic.snv_mnv.vcf.gz',
#'dkfz-brian/f72d245f-9340-4adf-8863-4ede9a7afbe4/35a74e53-16ff-4764-8397-6a9b02dfe733.dkfz-snvCalling_1-0-132-1.20150731.somatic.snv_mnv.vcf.gz');
#my @indel = ('broad-brian/a76d7d7a-6f19-4ae9-a152-7b909130946c/35a74e53-16ff-4764-8397-6a9b02dfe733.broad-snowman.20151216.somatic.indel.vcf.gz',
#'sanger-brian/0bac6371-e554-44b6-acb1-64e452db583f/35a74e53-16ff-4764-8397-6a9b02dfe733.svcp_1-0-5.20150707.somatic.indel.vcf.gz',
#'dkfz-brian/f72d245f-9340-4adf-8863-4ede9a7afbe4/35a74e53-16ff-4764-8397-6a9b02dfe733.dkfz-indelCalling_1-0-132-1.20150731.somatic.indel.vcf.gz');
#my @sv = ('broad-brian/a76d7d7a-6f19-4ae9-a152-7b909130946c/35a74e53-16ff-4764-8397-6a9b02dfe733.broad-dRanger_snowman.20151216.somatic.sv.vcf.gz',
#'sanger-brian/0bac6371-e554-44b6-acb1-64e452db583f/35a74e53-16ff-4764-8397-6a9b02dfe733.svcp_1-0-5.20150707.somatic.sv.vcf.gz',
#'dkfz-brian/f72d245f-9340-4adf-8863-4ede9a7afbe4/35a74e53-16ff-4764-8397-6a9b02dfe733.embl-delly_1-3-0-preFilter.20150731.somatic.sv.vcf.gz');

# Inputs: the filenames for SNVs for Broad, Sanger, DKFZ/EMBL;
# the filenames for INDELs for Broad, Sanger, DKFZ/EMBL;
# the filenames for SVs for Broad, Sanger, DKFZ/EMBL;
# the path to the root directory where all the files are;
# the path to the output directory.

# Call this script like this:
# perl vcf_cleaner.pl 35a74e53-16ff-4764-8397-6a9b02dfe733.broad-mutect.20151216.somatic.snv_mnv.vcf.gz \
#                                               35a74e53-16ff-4764-8397-6a9b02dfe733.svcp_1-0-5.20150707.somatic.snv_mnv.vcf.gz \
#                                               35a74e53-16ff-4764-8397-6a9b02dfe733.dkfz-snvCalling_1-0-132-1.20150731.somatic.snv_mnv.vcf.gz \
#                                               35a74e53-16ff-4764-8397-6a9b02dfe733.broad-snowman.20151216.somatic.indel.vcf.gz \
#                                               35a74e53-16ff-4764-8397-6a9b02dfe733.svcp_1-0-5.20150707.somatic.indel.vcf.gz \
#                                               35a74e53-16ff-4764-8397-6a9b02dfe733.dkfz-indelCalling_1-0-132-1.20150731.somatic.indel.vcf.gz \
#                                               35a74e53-16ff-4764-8397-6a9b02dfe733.broad-dRanger_snowman.20151216.somatic.sv.vcf.gz \
#                                               35a74e53-16ff-4764-8397-6a9b02dfe733.svcp_1-0-5.20150707.somatic.sv.vcf.gz \
#                                               35a74e53-16ff-4764-8397-6a9b02dfe733.embl-delly_1-3-0-preFilter.20150731.somatic.sv.vcf.gz \
#                                               /datastore/path_to_above_VCFs/ \
#                                               /datastore/output_directory 

my ($broad_snv, $sanger_snv, $de_snv,
        $broad_indel, $sanger_indel, $de_indel,
        $broad_sv, $sanger_sv, $de_sv,
        $in_dir, $out_dir) = @ARGV;

my @snv = ($broad_snv, $sanger_snv, $de_snv);
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
    print "processing file $i\n";
    process_file($in_dir."/".$i, $OUT);
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
  my $cmd = "sudo docker run --rm \\
        -v $file.vcf:/input.vcf:rw \\
        -v /datastore/refdata/public:/ref \\
        -v ~/vcflib/:/home/ngseasy/vcflib/ \\
        -v $out_dir:/outdir/:rw \\
        compbio/ngseasy-base:a1.0-002 /bin/bash -c \\
        \"vcf-sort /input.vcf > /outdir/$filename.sorted.vcf; \\
        echo \"zipping and indexing...\" \\
        bgzip -f /outdir/$filename.sorted.vcf ; \\
        tabix -p vcf /outdir/$filename.sorted.vcf.gz\"
   ";

  print "$cmd\n";

  my $result = system($cmd);

  print "Status of sort: $result\n";
}


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
    next if (!/PASS/ || /tier/);
    my @a = split /\t/;
    my $payload = "$a[0]\t$a[1]\t.\t$a[3]\t$a[4]\t$a[5]\t$a[6]\t.";
    $d->{$a[0]}{$a[1]} = $payload;
  }
  close IN;
}

