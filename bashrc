# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

export TERM=xterm-color

# Uncomment the following line if you don't like systemctl's auto-paging feature:
# export SYSTEMD_PAGER=


======
# User specific aliases and functions
# show full dir path 
PS1='\u@liangsvm:\w\$ '
alias vi='vim'
