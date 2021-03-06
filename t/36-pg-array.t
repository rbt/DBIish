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

my $sth = $dbh.do(q:to/STATEMENT/);
  DROP TABLE IF EXISTS sal_emp;
STATEMENT

$sth = $dbh.do(q:to/STATEMENT/);
  CREATE TABLE sal_emp (
    name               text,
    pay_by_quarter     integer[],
    schedule           text[][],
    salary_by_month    float[]
  );
STATEMENT

$sth = $dbh.do(q:to/STATEMENT/);
    INSERT INTO sal_emp
    VALUES (
      'Bill',
      ARRAY[10000, 10000, 10000, 1000],
      ARRAY[['meeting', 'lunch'], ['training day', 'presentation']],
      ARRAY[511.123, 622.345, 1]
    );
STATEMENT

$sth = $dbh.prepare(q:to/STATEMENT/);
    SELECT name, pay_by_quarter, schedule, salary_by_month
    FROM sal_emp
STATEMENT

$sth.execute;
my %h = $sth.row(:hash);

class SalEmp {
  has $.name;
  has Int @.pay_by_quarter;
  has @.schedule;
  has Num @.salary_by_month;
  submethod BUILD(:$!name, :@!pay_by_quarter, :@!schedule, :@!salary_by_month) { }
};

is %h.elems, 4, "Contain 4 elements";

my $obj;
lives-ok {
    $obj = SalEmp.new(|%h);
}, "Can create class";

isa-ok $obj.pay_by_quarter, Array[Int], 'Array[Int]';
isa-ok $obj.salary_by_month,  Array[Num], 'Array[Num]';
isa-ok $obj.schedule, Array, 'schedule is array';
is $obj.schedule.elems, 2,    'schedule with 2';
isa-ok $obj.schedule[0], Array[Str];

# Big integer
{
    $sth = $dbh.prepare(q{select ARRAY[12e12]::int8[]});
    $sth.execute();

    my ($col1) = $sth.row;
    is $col1.elems, 1,    '1 element';
    is $col1[0], '12000000000000', '12e12';
    is $col1[0].^name, 'Int', 'Is Integer';
}

# Big Floats may be returned in scientific notation from the DB
{
    $sth = $dbh.prepare(q{select ARRAY[12e32]::float[]});
    $sth.execute();

    my ($col1) = $sth.row;
    is $col1.elems, 1,    '1 element';
    is $col1[0], '1.2e+33', 'Big Float Value';
    is $col1[0].^name, 'Num', 'Is Number';
}

# Text can be anything except a null. With different encodings, every byte combination is possible.
{
    $sth = $dbh.prepare(q:to/STATEMENT/);
      SELECT ARRAY[(SELECT array_to_string(array_agg(chr(pos)), '')
                      FROM generate_series(1, 256) AS g(pos)),

                  $$\\"$$,
                  $$"\\$$,
                  NULL, 'NULL', ''
                  ];
STATEMENT
    $sth.execute();

    my ($col1) = $sth.row;
    is $col1.elems, 6,    '6 elements';
    is $col1[0].encode.elems, 385, 'String is expected length';
    is $col1[1], q{\\"}, 'Handle slash/quote correctly';
    is $col1[2], q{"\\}, 'Handle quote/slash correctly';
    is $col1[3].defined, False, 'undefined string';
    is $col1[4].defined, True, 'NULL value in string is defined';
    is $col1[4], q{NULL}, 'NULL value in string';
    is $col1[5], q{}, 'Empty string';
}

# Roundtrip corner-case values
{
    $sth = $dbh.prepare(q{SELECT ARRAY[?, ?, ?, ?, ?]});
    $sth.execute(q{"}, Nil, q{}, q{NULL}, q{\\});

    my ($col1) = $sth.row;
    is $col1.elems, 5,    '5 elements';
    is $col1[0], q{"}, 'Quote String value';
    is $col1[1].defined, False, 'undefined string';
    is $col1[2], q{}, 'Empty String';
    is $col1[3], q{NULL}, 'NULL value in string';
    is $col1[4], q{\\}, 'Backslash string value';
}

# Cleanup
$dbh.dispose;
