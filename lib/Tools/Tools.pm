package MMM::Tools::Tools;

use strict;
use warnings FATAL => 'all';
use English qw(FORMAT_NAME);
use Path::Class qw(dir);
use Log::Log4perl qw(:easy);


our $VERSION = '0.01';

=head1 NAME

MMM::Tools::Tools - functions for the mmm-tools.

=cut

=over 4

=item check_backup_destination($path, [$should_be_empty])

Check backup destination

=cut

sub check_backup_destination {
	my $dir = shift;
	my $should_be_empty = shift || 0;
	
	INFO "Checking local destination directory '$dir'...";

	system("mkdir -p $dir");
	unless (-d $dir && -x _ && -r _ && -w _) {
		ERROR "Destination dir '$dir' has invalid permissions (should be readable/writeable/executable)!";
		return 0;
	}

	if ($should_be_empty && scalar(glob("$dir/*"))) {
		ERROR "Destination dir '$dir' is not empty!";
		return 0;
	}

	INFO 'Directory is ok';
	return 1;
}

sub check_restore_source($) {
	my $dir = shift;
	
	INFO "Checking restore source directory '$dir'...";

	unless (-d $dir && -x _ && -r _) {
		ERROR "Source dir '$dir' has invalid permissions (should be readable/executable)!";
		return 0;
	}
	unless (scalar(glob("$dir/*"))) {
		ERROR "Source dir '$dir' is empty!";
		return 0;
	}
	unless (-d "$dir/_mmm" && -r _ && -x _) {
		ERROR "Source dir doesn't contain _mmm sub-directory!";
		return 0;
	}
	unless (-f "$dir/_mmm/status.txt" && -r _) {
		ERROR "$dir/_mmm/status.txt doesn't exist or isn't readable!";
		return 0;
	}
	unless (-f "$dir/_mmm/copy_method.txt" && -r _) {
		ERROR "$dir/_mmm/copy_method.txt doesn't exist or isn't readable!";
		return 0;
	}

	INFO 'Directory is ok';
	return 1;
}

sub check_restore_destination($) {
	my $dir = shift;
	
	INFO "Checking destination directory '$dir'...";

	system("mkdir -p $dir");
	unless (-d $dir && -x _ && -r _ && -w _) {
		ERROR "Destination dir '$dir' has invalid permissions (should be readable/writeable/executable)!";
		return 0;
	}

	INFO 'Directory is ok';
	return 1;
}
=item check_ssh_connection($host)

Check SSH connection to host $host.

=cut

sub check_ssh_connection($) {
	my $host = shift;

	my $ssh_host	= $main::config->{host}->{$host}->{ssh_user} . '@' . $main::config->{host}->{$host}->{ip};
	my $ssh_port	= $main::config->{host}->{$host}->{ssh_port};
	my $ssh_params	= $main::config->{host}->{$host}->{ssh_parameters};
	
	my $check_cmd	= "ssh $ssh_params -p $ssh_port $ssh_host date";

	INFO "Verifying ssh connection to remote host '$ssh_host' (command: $check_cmd)...";

	my $res = system($check_cmd);
	if ($res) {
		ERROR "Can't execute remote commands on host '$host' ($ssh_host): $!";
		return 0;
	}

	INFO "OK: SSH connection works fine!";
	return 1;
}


=item execute_remote($host, $program)

Execute command $program on remote host $host.

=cut

sub execute_remote($$) {
	my $host	= shift;
	my $program	= shift;

	my $ssh_host	= $main::config->{host}->{$host}->{ssh_user} . '@' . $main::config->{host}->{$host}->{ip};
	my $ssh_port	= $main::config->{host}->{$host}->{ssh_port};
	my $ssh_params	= $main::config->{host}->{$host}->{ssh_parameters};

	my $command		= $main::config->{host}->{$host}->{bin_path} . '/tools/' . $program;

	DEBUG "Executing $program on host '$host'...";
	INFO "ssh $ssh_params -p $ssh_port $ssh_host $command";

	chomp(my $res = `ssh $ssh_params -p $ssh_port $ssh_host $command`);
	print "$res\n";
	my @res_lines = split(/\n/, $res);
	my $last_line = pop(@res_lines);

	unless ($last_line =~ /^OK/) {
		ERROR $res;
		return 0;
	}
	return 1;
}


=item create_remote_snapshot($host)

Create snapshot on host $host.

=cut

sub create_remote_snapshot($) {
	my $host = shift;
	return execute_remote($host, 'create_snapshot');
}


=item remove_remote_snapshot($host)

Remove snapshot on host $host.

=cut

sub remove_remote_snapshot($) {
	my $host = shift;
	return execute_remote($host, 'remove_snapshot');
}


sub save_copy_method($$) {
	my $dir			= shift;
	my $copy_method	= shift;

	$dir .= '/_mmm';

	# Check config option
	if (! -d $dir) {
		ERROR "Directory _mmm doesn't exist!";
		return 0;
	}
	
	unless (open(F, ">$dir/copy_method.txt")) {
		ERROR "I/O Error while saving copy method!";
		return 0;
	}
	print F $copy_method;
	close(F);
	
	DEBUG "Saved copy method";
	return 1;
}


sub load_status($) {
	my $dir = shift;
	my $status;
	
	DEBUG 'Loading status info...';

	my $status_file = $dir . '/_mmm/status.txt';
	unless (-f $status_file && -r _) {
		ERROR "Status file '$status_file' doesn't exist or isn't readable!";
		return undef;
	}
	
	my $status_data = `cat $status_file`;

	my $VAR1;
	eval($status_data);
	if ($@) {
		ERROR "Can't parse status info: $@";
		return undef;
	}
	
	$status = $VAR1;
	
	my $method_file = $dir . '/_mmm/copy_method.txt';
	chomp(my $copy_method = `cat $method_file`);
	$status->{copy_method} = $copy_method;
	
	return \%{$status};
}


sub copy_clone_dirs($$$) {
	my $host		= shift;
	my $copy_method	= shift;
	my $dest_dir	= shift;

	my @clone_dirs = @{$main::config->{clone_dirs}};

	if  ($main::config->{copy_method}->{$copy_method}->{single_run}) {
		return copy_from_remote_single_run($host, $copy_method, $dest_dir, \@clone_dirs);
	}

	foreach my $sub_dir (@clone_dirs) {
		return 0 unless (copy_from_remote($host, $copy_method, $dest_dir, $sub_dir));
	}
	return 1;
}

sub copy_from_remote($$$$) {
	my $host		= shift;
	my $copy_method	= shift;
	my $dest_dir	= shift;
	my $sub_dir		= shift;

	my $host_info	= $main::config->{host}->{$host};
	my $ssh_host	= $host_info->{ssh_user} . '@' . $host_info->{ip};

	INFO "Copying '$sub_dir' from snapshot on host '$host' with copy method '$copy_method'";

	my $command = $main::config->{copy_method}->{$copy_method}->{backup_command};

	my $dir = dir('/', $sub_dir);
	unless ($dir->parent() eq '/') {
		$dest_dir .= '/' . $dir->parent();
		system("mkdir -p $dest_dir");
	}

	$command =~ s/%SSH_USER%/$host_info->{ssh_user}/ig;
	$command =~ s/%IP%/$host_info->{ip}/ig;
	$command =~ s/%SNAPSHOT%/$host_info->{lvm_mount_dir}/ig;
	$command =~ s/%DEST_DIR%/$dest_dir/ig;
	$command =~ s/%BACKUP_DIR%/$dest_dir/ig;
	$command =~ s/%CLONE_DIR%/$sub_dir/ig;

	INFO "Executing command $command";
	if (system($command)) {
		ERROR "Can't copy $sub_dir: $!";
		return 0;
	}
	
	INFO "Copied directory $sub_dir!";
	return 1;
}

sub copy_from_remote_single_run($$$$) {
	my $host		= shift;
	my $copy_method	= shift;
	my $dest_dir	= shift;
	my $clone_dirs	= shift;

	my $host_info	= $main::config->{host}->{$host};
	my $ssh_host	= $host_info->{ssh_user} . '@' . $host_info->{ip};
	
	INFO "Copying files from snapshot on host '$host' with copy method '$copy_method'";

	my $command = $main::config->{copy_method}->{$copy_method}->{backup_command};
	
	$command =~ s/%SSH_USER%/$host_info->{ssh_user}/ig;
	$command =~ s/%IP%/$host_info->{ip}/ig;
	$command =~ s/%SNAPSHOT%/$host_info->{lvm_mount_dir}/ig;
	$command =~ s/%DEST_DIR%/$dest_dir/ig;
	$command =~ s/%BACKUP_DIR%/$dest_dir/ig;

	if ($command =~ /!(.*)!/) {
		my $sub_tmpl = $1;
		my $sub_cmd = "";
		for my $sub_dir (@$clone_dirs) {
			my $partial = $sub_tmpl;
			$partial =~ s/%CLONE_DIR%/$sub_dir/ig;
			$sub_cmd .= " $partial";
		}
		
		$command =~ s/!.*!/$sub_cmd/;
	}
	
	INFO "Executing command $command";
	
	if (system($command)) {
# TODO New config entry "check command"?
#		system("rdiff-backup --check-destination-dir '$config->{dest_dir}'");
		ERROR "Can't copy from remote host: $!";
		return 0;
	}

	INFO sprintf("Copied directories '%s' from host '$host'!", join("', '", sort(@$clone_dirs)));
	return 1;
}


=item restore($copy_method, $src_dir, $dest_dir)

restore non-incremental backup

=cut

sub restore($$$) {
	my $copy_method	= shift;
	my $src_dir		= shift;
	my $dest_dir	= shift;

	# TODO check copy method

	if ($main::config->{copy_method}->{$copy_method}->{incremental}) {
		ERROR 'The backup directory contains an incremental backup! Use --version option to restore a specific version.';
		return 0;
	}

	my $command = $main::config->{copy_method}->{$copy_method}->{restore_command};
	
	$command =~ s/%SRC_DIR%/$src_dir/ig;
	$command =~ s/%BACKUP_DIR%/$src_dir/ig;
	$command =~ s/%DEST_DIR%/$dest_dir/ig;
	$command =~ s/%DATA_DIR%/$dest_dir/ig;
	INFO "Executing command $command";
	
	if (system($command)) {
		ERROR "Can't restore data: $!";
		return 0;
	}
	INFO "Restored backup from '$src_dir' to '$dest_dir'";
	return 1;
}


=item restore_incremental($copy_method, $src_dir, $dest_dir, $version)

restore incremental backup

=cut

sub restore_incremental($$$$) {
	my $copy_method	= shift;
	my $src_dir		= shift;
	my $dest_dir	= shift;
	my $version		= shift;

	# TODO check copy method

	unless ($main::config->{copy_method}->{$copy_method}->{incremental}) {
		ERROR 'The backup directory contains an non-incremental backup!';
		return 0;
	}

	my $command = $main::config->{copy_method}->{$copy_method}->{restore_command};
	
	$command =~ s/%SRC_DIR%/$src_dir/ig;
	$command =~ s/%BACKUP_DIR%/$src_dir/ig;
	$command =~ s/%DEST_DIR%/$dest_dir/ig;
	$command =~ s/%DATA_DIR%/$dest_dir/ig;
	$command =~ s/%VERSION%/$version/ig;
	INFO "Executing command $command";
	
	if (system($command)) {
		ERROR "Can't restore data: $!";
		return 0;
	}
	INFO "Restored backup version '$version' from '$src_dir' to '$dest_dir'";
	return 1;
}


=item list_increments($backup_dir, $copy_method)

list available backup increments

=cut

sub list_increments($$) {
	my $backup_dir	= shift;
	my $copy_method	= shift;

	my $command = $main::config->{copy_method}->{$copy_method}->{incremental_command};

	unless ($main::config->{copy_method}->{$copy_method}->{incremental}) {
		ERROR 'Invalid backup directory for incremental operations';
		exit(0);
	}
	
	$command =~ s/%BACKUP_DIR%/$backup_dir/ig;
	
	# List versions
	my $res = open(COMMAND, "$command|");
	unless ($res) {
		LogError("Can't read version info from backup!");
		exit(1);
	}

	my $line;
	if ($command =~ /rdiff-backup/ && $command =~ 'parsable-output') {
		# Beautify rdiff-backup output
		print "Following backup versions are available:\n";
		print "     Version | Date\n";
		print "-------------|---------------------------\n";

		my $timestamp;
format VERSION_LINE =
 @>>>>>>>>>> | @<<<<<<<<<<<<<<<<<<<<<<<<
 $timestamp,   scalar(localtime($timestamp))
.
		$FORMAT_NAME = 'VERSION_LINE';
		while ($line = <COMMAND>) {
			chomp $line;
			($timestamp,) = split(/\s+/, $line);
			write;
		}
	}
	else {
		while ($line = <COMMAND>) {
			print $line;
		}
	}
	close(COMMAND);
}


=item cleanup($status, $dir)

clean up restore directory

=cut
sub cleanup($$$) {

	my $status		= shift;
	my $dir			= shift;
	my $clone_dirs	= shift;

	INFO 'Cleaning dump from master.info and binary logs...';
	
	my $master_log = $status->{master}->{File};
	unless ($master_log =~ /^(.*)\.(\d+)$/) {
		ERROR "Unknown master binary log file name format '$master_log'!";
		return 0;
	}
	
	INFO "Deleting master binary logs: $1.*";
	system("find $dir -name '$1.*' | xargs rm -vf");
	
	if ($status->{slave} && $status->{slave}->{Relay_Log_File}) {
		my $slave_log = $status->{slave}->{Relay_Log_File};
		unless ($slave_log =~ /^(.*)\.(\d+)$/) {
			ERROR "Unknown relay binary log file name format '$slave_log'!";
			return 0;
		}
		INFO "Deleting relay binary logs: $1.*";
		system("find $dir -name '$1.*' | xargs rm -vf");
	}
	
	
	INFO 'Deleting .info and .pid  files...';
	system("find $dir -name master.info | xargs rm -vf");
	system("find $dir -name relay-log.info | xargs rm -vf");
	system("find $dir -name '*.pid' | xargs rm -vf");
	
	INFO 'Changing permissions on mysql data dir...';
	foreach my $sub_dir (@$clone_dirs) {
		system("chown -R mysql:mysql $dir/$sub_dir");
	}
	
	return 1;
}


=item get_host_by_ip($ip)

get hostname of host with ip $ip.

=cut

sub get_host_by_ip($) {
	my $ip = shift;
	foreach my $host (keys(%{$main::config->{host}})) {
		return $host if ($main::config->{host}->{$host}->{ip} eq $ip);
	}
	return '';
}


1;
