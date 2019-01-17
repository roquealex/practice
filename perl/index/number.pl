use strict;
use warnings;

my $filename = $ARGV[0];
open(my $fh, "$filename") or die "Could not open $filename $!";

my @sub_arr;
my @table_arr;

my @file = <$fh>;
#while (my $line = @file) {
foreach my $line (@file) {
  #print $line;
  if ($line =~ m/\s*"\s*#(#+)\s(.*?)\s*(\\n)?"/) {
    my $len = length($1);
    my $title = $2;
    if (scalar(@sub_arr) > $len) {
      while(scalar(@sub_arr)!=$len){
        pop(@sub_arr);
      }
      @sub_arr[-1]++;
    } elsif (scalar(@sub_arr) < $len) {
      while(scalar(@sub_arr)!=$len){
        push(@sub_arr,1);
      }
    } else {
      @sub_arr[-1]++;
    }
    my $sub = join('.',@sub_arr);
    #print("$sub $title\n");
    $title = "$sub $title";
    my $linkname = $title;
    $linkname =~ s/\s/-/g;
    my $entry = ' ' x (($len-1)*2)  . "* [$title](#$linkname)\n";
    push(@table_arr,$entry);
    $line =~ s/(#+)/$1 $sub/;
    #print $line;
    #print $len."\n"
  }
}
close($fh);

#open(my $ofh, ">$filename") or die "Could not open $filename $!";
foreach my $line (@file) {
  print $line;
}
#close($ofh);

foreach my $e (@table_arr) {
  print $e;
}

#my @sub_arr = (1,2,3);
#push(@sub_arr,4);
#print "@sub_arr\n";
#print "$sub_arr[-1]\n";
#my $dummy = <<'END';


my $dummy = << 'END'
while (my $line = <$fh>) {
  chomp $line;
  if ($line =~ m/^# (#+)\s*(.*?)\s*$/) {
    #print $line.": '$1' '$2'\n";
    my $level = 2;
    my $len = length($1);
    my $title = $2;
    my $linkname = $title;
    $linkname =~ s/\s/-/g;
    next if ($len < $level);
    print ' ' x (($len-$level)*2)  . "* [$title](#$linkname)\n";
  }
}
END



