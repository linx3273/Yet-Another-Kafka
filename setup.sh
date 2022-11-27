# !/bin/sh

# use bash ./setup.sh to run this script

yellow='\033[0;33m'
green='\033[1;32m'
nocolor='\033[0m'
cdir=$(pwd)

cd $(dirname "${BASH_SOURCE[0]}")
echo -e "${green}Working Dir: ${yellow}$cdir ${nocolor}"

echo -e "${green}Installing virtualenv library${nocolor}"
pip install virtualenv

echo -e "${green}Creating virtualenv${nocolor}"
python3 -m virtualenv .venv

echo -e "${green}Activating virtualenv${nocolor}"
source .venv/bin/activate

echo -e "${green}Installing dependencies${nocolor}"
pip install -r ./requirements.txt

# The commented lines below are not giving me the expected result, so I've let them stay..
# Will probably work on them later

# echo -e "${green}Creating activate.sh${nocolor}"
# echo -e "# !/bin/sh\nsource .venv/bin/activate" > activate.sh

# echo -e "${green}Setting up permissions${nocolor}"
# chmod u+x ./activate.sh

# echo -e "${green}Done. Use 'source .venv/bin/activate' to enable the virtualenv'${nocolor}"

echo -e "${green}Done${nocolor}"