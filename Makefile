
ifndef INSTALLDIR
INSTALLDIR = installvendorlib
endif

MODULEDIR = $(DESTDIR)$(shell eval "`perl -V:${INSTALLDIR}`"; echo "$$${INSTALLDIR}")/MMM
BINDIR    = $(DESTDIR)/usr/lib/mysql-mmm
SBINDIR   = $(DESTDIR)/usr/sbin
LOGDIR    = $(DESTDIR)/var/log/mysql-mmm
ETCDIR    = $(DESTDIR)/etc
CONFDIR   = $(ETCDIR)/mysql-mmm

install_common:
		mkdir -p $(DESTDIR) $(MODULEDIR) $(BINDIR) $(SBINDIR) $(LOGDIR) $(ETCDIR) $(CONFDIR) $(ETCDIR)/init.d/
		cp -r lib/Common/ $(MODULEDIR)
		[ -f $(CONFDIR)/mmm_common.conf ] || cp etc/mysql-mmm/mmm_common.conf $(ETCDIR)/mysql-mmm/

install_agent: install_common
		mkdir -p $(BINDIR)/agent/
		cp -r lib/Agent/ $(MODULEDIR)
		cp -r bin/agent/* $(BINDIR)/agent/
		cp -r etc/init.d/mysql-mmm-agent $(ETCDIR)/init.d/
		cp sbin/mmm_agentd $(SBINDIR)
		[ -f $(CONFDIR)/mmm_agent.conf  ] || cp etc/mysql-mmm/mmm_agent.conf  $(ETCDIR)/mysql-mmm/

install_monitor: install_common
		mkdir -p $(BINDIR)/monitor/
		cp -r lib/Monitor/ $(MODULEDIR)
		cp -r bin/monitor/* $(BINDIR)/monitor/
		cp -r etc/init.d/mysql-mmm-monitor $(ETCDIR)/init.d/
		cp sbin/mmm_control sbin/mmm_mond $(SBINDIR)
		[ -f $(CONFDIR)/mmm_mon.conf    ] || cp etc/mysql-mmm/mmm_mon.conf    $(ETCDIR)/mysql-mmm/

install_tools: install_common
		mkdir -p $(BINDIR)/tools/
		cp -r lib/Tools/ $(MODULEDIR)
		cp -r bin/tools/* $(BINDIR)/tools/
		cp sbin/mmm_backup sbin/mmm_clone sbin/mmm_restore $(SBINDIR)
		[ -f $(CONFDIR)/mmm_tools.conf  ] || cp etc/mysql-mmm/mmm_tools.conf  $(ETCDIR)/mysql-mmm/

install: install_agent install_monitor install_tools
