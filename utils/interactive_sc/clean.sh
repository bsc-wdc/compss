rm -rf build
rm -rf dist
rm -rf pycompss_interactive_sc.egg-info
rm -rf pycompss_interactive_sc/__pycache__
cat files.txt | xargs rm -rf
rm files.txt
