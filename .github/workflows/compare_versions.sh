read package_name

echo "Finding version numbers for $package_name"

conda_version=$(conda search -c conda-forge $package_name | tail -n1 | awk '{print $2}')
echo "conda version=$conda_version"

pypi_version=$(get_pypi_latest_version $package_name)
echo "pypi version=$pypi_version"

if ["$conda_version" != "$pypi_version"]
then 
    exit 1