set term=dtterm
set encoding=prc
set fileencoding=prc
set fileencodings=utf8,prc
"colorscheme leonscolor
hi Comment ctermfg=DarkCyan


set nocompatible
syntax on
set nu " show line number
set bg=dark " background is dark
set hls " highlight the search keywords
set fileformat=unix "use unix file format
set mps+=<:> "include the < > as match pairs
set tabstop=4 "a tab represent 4 space
set shiftwidth=4 "auto indent 4 space
set showcmd

set autoindent "auto indent

“highlight the charater that over 80 in the line
highlight OverLength ctermbg=red ctermfg=white guibg=#592929
match OverLength /\%81v.\+/

"set nowrap "no wrap in long line
"set ruler " Always show current positions along the bottom
"set ignorecase smartcase " easier to ignore case for searching
"set noerrorbells " don't make noise
"set showmatch " show matching brackets while insert a bracker
"set mat=5 " how many tenths of a second to blink matching brackets for
