package MMM::Tools::Snapshot::LVM;

use strict;
use warnings FATAL => 'all';
use Log::Log4perl qw(:easy);


sub create() {
	my $this = $main::config->{this};
	my $host = $main::config->{host}->{$this};

	my @command = (
		$host->{lvm_bin_lvcreate},
		'--snapshot', 
		'--size', $host->{lvm_snapshot_size}, 
		'--name', 'mmm_snapshot', 
		join('/', '/dev' , $host->{lvm_volume_group}, $host->{lvm_logical_volume})
	);
	
	my $lvm_res = system(@command);
	INFO "lvcreate output: '$lvm_res'";
	
	return "ERROR: Can't create snapshot: $!" if ($lvm_res);

	my $mount_opts = $host->{lvm_mount_opts};
	$mount_opts = '-o rw' unless ($mount_opts);

	my $res = system(sprintf('mount %s /dev/%s/mmm_snapshot %s', $mount_opts, $host->{lvm_volume_group}, $host->{lvm_mount_dir}));

	return "ERROR: Can't mount snapshot: $!" if ($res);
	return 'OK: Snapshot createg!';
}


sub remove() {
	my $this = $main::config->{this};
	my $host = $main::config->{host}->{$this};
	if (!$host) {
		return "ERROR: Invalid 'this' value: '$this'!";
	}

	# Unmount snapshot
	my $res = system('umount', $host->{lvm_mount_dir});
	return "ERROR: Can't umount snapshot: $!" if ($res);

	my @command = (
		$host->{lvm_bin_lvremove},
		'-f',
		join('/', '/dev', $host->{lvm_volume_group}, 'mmm_snapshot')
	);
	my $lvm_res = system(@command);
	INFO "lvremove output: '$lvm_res'";
	
	return  "ERROR: Can't remove snapshot: $!" if ($lvm_res);
	return 'OK: Snapshot removed!';
}

1;
