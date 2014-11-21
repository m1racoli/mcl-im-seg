function rc = myImplay(filename)

data = load(filename)
l = length(data.ss0);

for i = 1:l;
  subplot(2,2,1)
  imagesc(data.ss0(i).Z);
  subplot(2,2,2)
  %m = max(max(data.ss0(i).intenSR));
  imagesc(data.ss0(i).intenSR);
  subplot(2,2,3)
  imagesc(data.ss0(i).X);
  subplot(2,2,4)
  imagesc(data.ss0(i).Y);
  drawnow;
end

end
