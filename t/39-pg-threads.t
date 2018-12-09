use v6;
use Test;
use DBIish;

plan 4;

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

ok $dbh, 'Connected';

# Be less verbose;
$dbh.do('SET client_min_messages TO WARNING');
lives-ok { $dbh.do('DROP TABLE IF EXISTS test_thread') }, 'Clean';
lives-ok {
    $dbh.do(q|
	CREATE TABLE test_thread (
	acolumn text, perlthread text, pgbackend int4,
	createts timestamp with time zone not null default current_timestamp)
	|)
}, 'Table created';

# Check that it is possible to work with the database from multiple threads
# at once with a connection object per thread
subtest 'Multiple connections, one per thread' => {
    my @inserters = do for ^5 -> $thread {
        start {
            my $dbht = DBIish.connect('Pg', |%con-parms);

            for ^100 {
                my $sth = $dbht.prepare(q:to/STATEMENT/);
                    INSERT INTO test_thread (acolumn, perlthread, pgbackend)
                    VALUES (?, ?, pg_backend_pid())
                    STATEMENT
                $sth.execute((('a'..'z').pick xx 40).join, $*THREAD.id);
                $sth.finish;
            }
            $dbht.dispose;
        }
    }

    for @inserters.kv -> $idx, $i {
        lives-ok { await $i }, "Inserting thread $idx completed";
    }

    given $dbh.prepare('SELECT COUNT(*) FROM test_thread') -> $sth {
        $sth.execute;
        is $sth.row()[0], 500, 'Correct number of rows were inserted';
        $sth.finish;
    }

    $dbh.do('DROP TABLE IF EXISTS test_thread');
}
