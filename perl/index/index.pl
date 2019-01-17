use strict;
use warnings;

my $filename = $ARGV[0];
open(my $fh, "$filename") or die "Could not open $filename $!";

my @table_arr;

while (my $line = <$fh>) {
  #print $line;
  if ($line =~ m/\s*"\s*#(#+)\s(.*?)\s*(\\n)?"/) {
    my $len = length($1);
    my $title = $2;
    #print("$sub $title\n");
    my $linkname = $title;
    $linkname =~ s/\s/-/g;
    my $entry = ' ' x (($len-1)*2)  . "* [$title](#$linkname)\n";
    push(@table_arr,$entry);
  }
}
close($fh);

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



