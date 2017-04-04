# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

export TERM=xterm-color

# Uncomment the following line if you don't like systemctl's auto-paging feature:
# export SYSTEMD_PAGER=

# User specific aliases and functions
PS1='\u@mywsssss:\w\$ '
alias vi='vim'
alias ll='ls -lrt'

export PYTHONSTARTUP=~/.pythonrc.py

# by default, use mux default layout
[ -z "$TMUX" ] && mux default


