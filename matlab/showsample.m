function rc = showsample(filename,n)

if nargin < 2 || isempty(n)
  n = 1;
end

data = load(filename);
l = length(data.ss0);
ns= n*n;
p = ns+1;

colormap('gray');

for j = 1:n;
  for i = 1:n;
	frame = i + (j-1)*n;
    val = i*2-1 + (j-1)*n*2;
    index = max(1,min(l,int32(frame*l/p)));
    subplot(n,2*n,val);    
    imagesc(-data.ss0(index).Z, [-2.5,0]);
	title(index);
	subplot(n,2*n,val+1);
    imagesc(data.ss0(index).intenSR, [0,4000]);
	title(index);
  end
end

end