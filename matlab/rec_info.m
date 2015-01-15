function [] = rec_info(ss0)

info = zeros(4,3);
info(:,1) = Inf;

l = length(ss0);
[h w] = size(ss0(1).X);
fprintf('frame: %d, height: %d, width: %d\n',l,h,w);

for i = 1:l
	f = ss0(i);
	info(:,1) = min(info(:,1),[mmin(f.X);mmin(f.Y);mmin(f.Z);mmin(f.intenSR)]);
	info(:,2) = sum([info(:,2) [msum(f.X);msum(f.Y);msum(f.Z);msum(f.intenSR)]],2);
	info(:,3) = max(info(:,3),[mmax(f.X);mmax(f.Y);mmax(f.Z);mmax(f.intenSR)]);
end

info(:,2) = info(:,2) ./ (l*h*w);

fprintf('       min       mean       max\n')
fprintf('X: [%+9.6f %+9.6f %+9.6f]\n',info(1,:));
fprintf('Y: [%+9.6f %+9.6f %+9.6f]\n',info(2,:));
fprintf('Z: [%+9.6f %+9.6f %+9.6f]\n',info(3,:));
fprintf('I: [%+9.2f %+9.2f %+9.2f]\n',info(4,:));

end

function m = mmin(M)
m = min(min(M));
end

function m = mmax(M)
m = max(max(M));
end

function m = msum(M)
m = sum(sum(M));
end