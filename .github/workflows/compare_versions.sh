echo "Finding version numbers for $1"

conda_version=$(conda search -c conda-forge $1 | tail -n1 | awk '{print $2}')
echo "conda version=$conda_version"

pypi_version=$(get_pypi_latest_version $1)
echo "pypi version=$pypi_version"

if ["$conda_version" != "$pypi_version"]
then 
    exit 1