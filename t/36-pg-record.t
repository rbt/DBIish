#!/usr/bin/env perl6

use v6;
use DBIish;
use Test;

plan 27;
my %con-parms;
# If env var set, no parameter needed.
%con-parms<database> = 'dbdishtest' unless %*ENV<PGDATABASE>;
%con-parms<user> = 'postgres' unless %*ENV<PGUSER>;
my $dbh;

try {
  $dbh = DBIish.connect('Pg', |%con-parms);
  CATCH {
        when X::DBIish::LibraryMissing | X::DBDish::ConnectionFailed {
        diag "$_\nCan't continue.";
        }
            default { .throw; }
  }
}
without $dbh {
    skip-rest 'prerequisites failed';
    exit;
}

$dbh.do(q:to/STATEMENT/);
  DROP TYPE IF EXISTS dbdishrec;
STATEMENT

$dbh.do(q:to/STATEMENT/);
  CREATE TYPE dbdishrec AS (var1 text, var2 int8);
STATEMENT

my $sth = $dbh.prepare(q:to/STATEMENT/);
    SELECT ($$some complex" '\n string$$, 1234)::dbdishrec AS col;
STATEMENT

$sth.execute;
my %h = $sth.row(:hash);

is %h.elems, 1, "Contain 1 elements";

my @rec = $dbh.pg-decode-record(%h<col>);

is @rec.elems, 2, "Record contained 2 elements";

is @rec[0], qq{some complex" '\n string}, 'Escaped items unescaped';
is @rec[1], '1234', '';

# Don't actually want a string but without asking the DB for the structure of dbdishrec 
# this is the best that can be done.
ok @rec[1].isa(Str), 'Item 2 is a string';


$dbh.do(q:to/STATEMENT/);
  DROP TYPE IF EXISTS dbdishrec;
STATEMENT

# Cleanup
$dbh.dispose;
