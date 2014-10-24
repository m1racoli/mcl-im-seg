function rc = showsample(filename,n)

if nargin < 2 || isempty(n)
  n = 1;
end

data = load(filename);
l = length(data.ss0);
ns= n*n;
p = ns+1;

colormap('gray');

for i = 1:n;
  for j = 1:n;
    val = i + (j-1)*n;
    index = max(1,min(l,int32(val*l/p)));
    subplot(n,n,val);    
    imagesc(-data.ss0(index).Z, [-2.5,0]);
	title(index);
  end
end

end