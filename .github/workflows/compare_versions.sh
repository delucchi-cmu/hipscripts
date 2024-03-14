some_failure=false

for package_name in "hipscat" "hipscat-import" "lsdb"
do
    echo "Finding version numbers for $package_name"

    conda_version=$(conda search -c conda-forge $package_name | tail -n1 | awk '{print $2}')
    echo "conda version=$conda_version"

    pypi_version=$(get_pypi_latest_version $package_name)
    echo "pypi version=$pypi_version"

    if ["$conda_version" != "$pypi_version"]
    then 
        some_failure=true
    fi
done

if ["$some_failure" == true]
then
    exit 1
fi