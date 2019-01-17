use strict;
use warnings;

my $filename = "kbWindNorYuc.py";
open(my $fh, "$filename") or die "Could not open $filename $!";

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
close($fh);




