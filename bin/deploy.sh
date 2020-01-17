
home_dir=`pwd`
base_dir=$(dirname $0)/..
cd $base_dir
base_dir=`pwd`

mvn clean package
mkdir -p $base_dir/deploy/samza
tar -xvf $base_dir/target/hello-samza-1.4.0-SNAPSHOT-dist.tar.gz -C $base_dir/deploy/samza
